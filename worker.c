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
//just empty it and grant the request.

// 6. Implement a wait queue for I/O delay on needing to swap a process out.

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

#define MSG_TERMINATE 0
#define MSG_MEMORY_REQUEST 1
#define MSG_GRANTED 1

const size_t BUFF_SZ = sizeof(unsigned int) * 2;

struct Message {
    long mtype;
    int value;
    int pid;
    int slot;
    int address;
    int requestType;
};

//Convert the shared memory clock to total nanoseconds.
long long getClockNS(unsigned int *clock) {
    return ((long long)clock[0] * NANOPERSEC) + clock[1];
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "worker: expected time limit seconds and nanoseconds\n");
        return EXIT_FAILURE;
    }

    int timeLimitSec = atoi(argv[1]);
    int timeLimitNano = atoi(argv[2]);

    if (timeLimitSec < 0) {
        fprintf(stderr, "worker: invalid time limit seconds\n");
        return EXIT_FAILURE;
    }

    if (timeLimitNano < 0 || timeLimitNano >= NANOPERSEC) {
        fprintf(stderr, "worker: invalid time limit nanoseconds\n");
        return EXIT_FAILURE;
    }

    long long timeLimitNS = ((long long)timeLimitSec * NANOPERSEC) + timeLimitNano;

    if (timeLimitNS <= 0) {
        fprintf(stderr, "worker: time limit is not positive\n");
        return EXIT_FAILURE;
    }

    key_t shm_key = ftok("oss.c", 0);

    if (shm_key == (key_t) -1) {
        perror("worker: Error in ftok for shared memory");
        return EXIT_FAILURE;
    }

    int shm_id = shmget(shm_key, BUFF_SZ, 0700);

    if (shm_id == -1) {
        perror("worker: Error in shmget");
        return EXIT_FAILURE;
    }

    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);

    if (clock == (void *) -1) {
        perror("worker: Error in shmat");
        return EXIT_FAILURE;
    }

    key_t msg_key = ftok("oss.c", 1);

    if (msg_key == (key_t) -1) {
        perror("worker: Error in ftok for message queue");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    int msg_id = msgget(msg_key, 0700);

    if (msg_id == -1) {
        perror("worker: Error in msgget");

        if (shmdt(clock) == -1) {
            perror("worker: Error in shmdt");
        }

        return EXIT_FAILURE;
    }

    long long startTimeNS = getClockNS(clock);

    printf("worker: attached to shared memory clock\n");
    printf("worker: starting clock is %u:%u\n", clock[0], clock[1]);
    printf("worker: time limit is %d seconds and %d nanoseconds\n",
           timeLimitSec,
           timeLimitNano);

    srand((unsigned int)(getpid() + clock[1]));

    while (1) {
        struct Message msgFromOSS;

        //Wait for OSS to give this worker a turn.
        if (msgrcv(msg_id,
                   &msgFromOSS,
                   sizeof(struct Message) - sizeof(long),
                   getpid(),
                   0) == -1) {
            perror("worker: Error in msgrcv turn message");

            if (shmdt(clock) == -1) {
                perror("worker: Error in shmdt");
            }

            return EXIT_FAILURE;
        }

        long long currentTimeNS = getClockNS(clock);

        //If this worker has reached its time limit, tell OSS it is done.
        if (currentTimeNS - startTimeNS >= timeLimitNS) {
            struct Message doneMessage;

            doneMessage.mtype = 1;
            doneMessage.value = MSG_TERMINATE;
            doneMessage.pid = getpid();
            doneMessage.slot = msgFromOSS.slot;
            doneMessage.address = 0;
            doneMessage.requestType = REQUEST_READ;

            if (msgsnd(msg_id,
                       &doneMessage,
                       sizeof(struct Message) - sizeof(long),
                       0) == -1) {
                perror("worker: Error in msgsnd terminate message");

                if (shmdt(clock) == -1) {
                    perror("worker: Error in shmdt");
                }

                return EXIT_FAILURE;
            }

            break;
        }

        int page = rand() % PAGE_COUNT;
        int offset = rand() % PAGE_SIZE;
        int address = (page * PAGE_SIZE) + offset;

        int roll = rand() % 100;
        int requestType;

        if (roll < 70) {
            requestType = REQUEST_READ;
        } else {
            requestType = REQUEST_WRITE;
        }

        struct Message requestMessage;

        requestMessage.mtype = 1;
        requestMessage.value = MSG_MEMORY_REQUEST;
        requestMessage.pid = getpid();
        requestMessage.slot = msgFromOSS.slot;
        requestMessage.address = address;
        requestMessage.requestType = requestType;

        if (requestType == REQUEST_WRITE) {
            printf("worker: requesting write of address %d\n", address);
        } else {
            printf("worker: requesting read of address %d\n", address);
        }

        if (msgsnd(msg_id,
                   &requestMessage,
                   sizeof(struct Message) - sizeof(long),
                   0) == -1) {
            perror("worker: Error in msgsnd memory request");

            if (shmdt(clock) == -1) {
                perror("worker: Error in shmdt");
            }

            return EXIT_FAILURE;
        }

        //Wait for OSS to grant the request.
        //If this was a page fault, this wait may represent disk I/O delay.
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

        if (grantMessage.value == MSG_GRANTED) {
            printf("worker: request for address %d was granted by oss\n",
                   grantMessage.address);
        }
    }

    if (shmdt(clock) == -1) {
        perror("worker: Error in shmdt");
        return EXIT_FAILURE;
    }

    printf("worker: detached from shared memory and exiting\n");

    return EXIT_SUCCESS;
}