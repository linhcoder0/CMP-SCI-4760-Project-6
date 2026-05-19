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

#define NANOPERSEC 1000000000LL

// Same shared memory clock size as oss.c.
//
// clock[0] is seconds.
// clock[1] is nanoseconds.
const size_t BUFF_SZ = sizeof(unsigned int) * 2;

// Same message structure as oss.c.
struct Message {
    long mtype;
    int value;
    int pid;
    int slot;
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

    // validTimeLimit starts as true.
    // If any part of the time limit looks wrong, set it to false.
    int validTimeLimit = 1;

    if (timeLimitSec < 0) {
        validTimeLimit = 0;
    }

    if (timeLimitNano < 0 || timeLimitNano >= NANOPERSEC) {
        validTimeLimit = 0;
    }

    // Generate the same shared memory key that oss used.
    key_t shm_key = ftok("oss.c", 0);

    if (shm_key == (key_t) -1) {
        perror("worker: Error in ftok for shared memory");
        return EXIT_FAILURE;
    }

    // Find the shared memory segment that oss already created.
    int shm_id = shmget(shm_key, BUFF_SZ, 0700);

    if (shm_id == -1) {
        perror("worker: Error in shmget");
        return EXIT_FAILURE;
    }

    // Attach worker to the shared memory clock.
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

    // Read the shared memory clock.
    // This proves the worker can see the same clock as oss.
    long long currentTimeNS = getClockNS(clock);
    long long timeLimitNS = ((long long)timeLimitSec * NANOPERSEC) + timeLimitNano;

    if (timeLimitNS <= 0) {
        validTimeLimit = 0;
    }

    if (currentTimeNS < 0) {
        validTimeLimit = 0;
    }

    // Generate the same message queue key that oss used.
    key_t msg_key = ftok("oss.c", 1);

    if (msg_key == (key_t) -1) {
        perror("worker: Error in ftok for message queue");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    // Get the message queue that oss already created.
    int msg_id = msgget(msg_key, 0700);

    if (msg_id == -1) {
        perror("worker: Error in msgget");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    // Wait for a message from oss.
    // oss sends the message with mtype equal to this worker's pid.
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

    printf("worker: received message from oss\n");
    printf("worker: message value is %d, slot is %d\n",
           msgFromOSS.value,
           msgFromOSS.slot);

    // Send a reply back to oss.
    // oss waits for replies using message type 1.
    struct Message msgToOSS;

    msgToOSS.mtype = 1;
    msgToOSS.value = validTimeLimit;
    msgToOSS.pid = getpid();
    msgToOSS.slot = msgFromOSS.slot;

    if (msgsnd(msg_id,
               &msgToOSS,
               sizeof(struct Message) - sizeof(long),
               0) == -1) {
        perror("worker: Error in msgsnd");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    printf("worker: sent reply to oss\n");

    // Worker only detaches from shared memory.
    // Worker does not remove shared memory.
    // Worker does not remove the message queue.
    // oss owns IPC cleanup.
    if (shmdt(clock) == -1) {
        perror("worker: Error in shmdt");
        return EXIT_FAILURE;
    }

    printf("worker: detached from shared memory and exiting\n");

    return EXIT_SUCCESS;
}