// 1. Get a makefile that compiles two source files,
//have oss allocate shared memory, use it, then deallocate it. Make sure to
// check all possible error returns.

// 2. Get oss to fork off and exec one child and have that child
//attach to shared memory and check the clock and verify it
// has correct resource limit. Then test having child and oss
//communicate through message queues. Set up PCB and frame
// table/page tables

// 3. Have child request a read/write of a memory address
//(just using the first scheme) and have oss always grant it and log it.

// 4. Set up more than one process going through your system, still granting all requests.

// 5. Now start filling out your page table and frame table; if a frame is full,
//just empty it (indicating in the process that you
// took it from that it is gone) and grant the request.

// 6. Implement a wait queue for I/O delay on needing to swap a process out.

// 7. Do not forget that swapping out a process with a dirty bit should take more time on your device

// 8. Implement the FIFO scheme

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <time.h>

#define NANOPERSEC 1000000000LL

#define PAGE_COUNT 16
#define PAGE_SIZE 1024

#define REQUEST_READ 0
#define REQUEST_WRITE 1

// Same shared memory clock size as oss.c.
//
// clock[0] is seconds.
// clock[1] is nanoseconds.
const size_t BUFF_SZ = sizeof(unsigned int) * 2;

struct Message {
    //Same message structure as oss.c.
    //Both programs must use the same fields in the same order.

    long mtype;
    //message type.
    //Worker receives messages where mtype is the worker's pid.
    //Worker sends messages to OSS using mtype = 1.

    int value;
    //value = 1 means normal message or granted request.

    int pid;
    //pid of the process sending the message.

    int slot;
    //PCB slot assigned by OSS.

    int address;
    //Logical address requested by this worker.

    int requestType;
    //REQUEST_READ means read.
    //REQUEST_WRITE means write.
};

// Convert the shared memory clock to total nanoseconds.
long long getClockNS(unsigned int *clock) {
    return ((long long)clock[0] * NANOPERSEC) + clock[1];
}

int main(int argc, char *argv[]) {
    // Worker expects two arguments from oss.
    // argv[1] is the time limit seconds.
    // argv[2] is the time limit nanoseconds.
    if (argc < 3) {
        fprintf(stderr, "worker: expected time limit seconds and nanoseconds\n");
        return EXIT_FAILURE;
    }

    int timeLimitSec = atoi(argv[1]);
    int timeLimitNano = atoi(argv[2]);

    //Check that the time limit passed by OSS is valid.
    //This was part of step 2, and we keep it here.
    if (timeLimitSec < 0) {
        fprintf(stderr, "worker: invalid time limit seconds\n");
        return EXIT_FAILURE;
    }

    if (timeLimitNano < 0 || timeLimitNano >= NANOPERSEC) {
        fprintf(stderr, "worker: invalid time limit nanoseconds\n");
        return EXIT_FAILURE;
    }

    //Generate the same shared memory key that oss used.
    key_t shm_key = ftok("oss.c", 0);

    if (shm_key == (key_t) -1) {
        perror("worker: Error in ftok for shared memory");
        return EXIT_FAILURE;
    }

    //Find the shared memory segment that oss already created.
    int shm_id = shmget(shm_key, BUFF_SZ, 0700);

    if (shm_id == -1) {
        perror("worker: Error in shmget");
        return EXIT_FAILURE;
    }

    //Attach worker to the shared memory clock.
    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);

    if (clock == (void *) -1) {
        perror("worker: Error in shmat");
        return EXIT_FAILURE;
    }

    printf("worker: attached to shared memory clock\n");
    printf("worker: clock is %u:%u\n", clock[0], clock[1]);
    printf("worker: time limit argument is %d seconds and %d nanoseconds\n",
           timeLimitSec,
           timeLimitNano);

    //Read the shared memory clock.
    //This proves the worker can still see the same simulated clock as OSS.
    long long currentTimeNS = getClockNS(clock);
    long long timeLimitNS = ((long long)timeLimitSec * NANOPERSEC) + timeLimitNano;

    if (timeLimitNS <= 0) {
        fprintf(stderr, "worker: time limit is not positive\n");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    if (currentTimeNS < 0) {
        fprintf(stderr, "worker: clock value is invalid\n");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    //Generate the same message queue key that oss used.
    key_t msg_key = ftok("oss.c", 1);

    if (msg_key == (key_t) -1) {
        perror("worker: Error in ftok for message queue");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    //Get the message queue that oss already created.
    int msg_id = msgget(msg_key, 0700);

    if (msg_id == -1) {
        perror("worker: Error in msgget");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    //Wait for OSS to tell this worker to take a turn.
    //OSS sends the message with mtype equal to this worker's pid.
    struct Message msgFromOSS;

    if (msgrcv(msg_id,
               &msgFromOSS,
               sizeof(struct Message) - sizeof(long),
               getpid(),
               0) == -1) {
        perror("worker: Error in msgrcv");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    printf("worker: received turn message from oss\n");
    printf("worker: message value is %d, slot is %d\n",
           msgFromOSS.value,
           msgFromOSS.slot);

    //Seed the random number generator.
    //The pid helps make each worker different later when there are many workers.
    srand((unsigned int)(getpid() + clock[1]));

    //Generate a random page number from 0 to 15.
    //
    //This matches the assignment's first scheme:
    //Each process has 16 pages.
    int page = rand() % PAGE_COUNT;

    //Generate a random offset from 0 to 1023.
    int offset = rand() % PAGE_SIZE;

    //Combine page and offset into one logical memory address.
    //
    //address = page * 1024 + offset.
    int address = (page * PAGE_SIZE) + offset;

    //Randomly decide read or write.
    //
    //This is biased toward reads.
    //Numbers 0 through 69 become reads.
    //Numbers 70 through 99 become writes.
    int roll = rand() % 100;
    int requestType;

    if (roll < 70) {
        requestType = REQUEST_READ;
    } else {
        requestType = REQUEST_WRITE;
    }

    //Create the memory request message for OSS.
    struct Message msgToOSS;

    //OSS receives worker messages using mtype = 1.
    msgToOSS.mtype = 1;

    //value = 1 means this is a normal memory request.
    msgToOSS.value = 1;

    //The worker is the sender.
    msgToOSS.pid = getpid();

    //Send back the same PCB slot OSS gave us.
    msgToOSS.slot = msgFromOSS.slot;

    //Send the generated logical address.
    msgToOSS.address = address;

    //Send whether this is a read or write.
    msgToOSS.requestType = requestType;

    if (requestType == REQUEST_WRITE) {
        printf("worker: requesting write of address %d\n", address);
    } else {
        printf("worker: requesting read of address %d\n", address);
    }

    //Send the memory request to OSS.
    if (msgsnd(msg_id,
               &msgToOSS,
               sizeof(struct Message) - sizeof(long),
               0) == -1) {
        perror("worker: Error in msgsnd memory request");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    //Wait for OSS to grant the memory request.
    //
    //OSS sends the grant using this worker's pid as mtype.
    struct Message grantMessage;

    if (msgrcv(msg_id,
               &grantMessage,
               sizeof(struct Message) - sizeof(long),
               getpid(),
               0) == -1) {
        perror("worker: Error in msgrcv grant message");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    //Check whether OSS granted the request.
    if (grantMessage.value == 1) {
        printf("worker: request for address %d was granted by oss\n",
               grantMessage.address);
    } else {
        printf("worker: request for address %d was not granted by oss\n",
               address);
    }

    //Worker only detaches from shared memory.
    //Worker does not remove shared memory.
    //Worker does not remove the message queue.
    //OSS owns IPC cleanup.
    if (shmdt(clock) == -1) {
        perror("worker: Error in shmdt");
        return EXIT_FAILURE;
    }

    printf("worker: detached from shared memory and exiting\n");

    return EXIT_SUCCESS;
}