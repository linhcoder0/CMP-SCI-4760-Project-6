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
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>

#define NANOPERSEC 1000000000LL
#define MAX_LOG_LINES 10000

#define MAX_PCB_SIZE 18
#define PAGE_COUNT 16
#define FRAME_COUNT 64

//Each process has 16K logical memory.
//Each page is 1K.
//So each process has 16 pages, and each page has 1024 possible offsets.
#define PAGE_SIZE 1024

//These values are used in the message structure.
//REQUEST_READ means the child is requesting a simulated read.
//REQUEST_WRITE means the child is requesting a simulated write.
#define REQUEST_READ 0
#define REQUEST_WRITE 1

//This is how much time OSS adds before sending a worker a turn message.
//This keeps the simulated clock moving.
#define TURN_INCREMENT_NS LOOP_INCREMENT_NS

//This is how much time OSS adds when it grants a memory request.
//Step 3 says normal memory access is granted immediately.
//Later, page faults will take much longer.
#define MEMORY_ACCESS_NS 100LL

//This is used when OSS has no active process to message but still needs time to move.
#define IDLE_INCREMENT_NS 10000LL

#define DISK_IO_NS 14000000LL
#define DIRTY_SWAP_EXTRA_NS 14000000LL
#define LOOP_INCREMENT_NS 10000000LL

#define MSG_TERMINATE 0
#define MSG_MEMORY_REQUEST 1
#define MSG_GRANTED 1

#define MAX_IO_QUEUE 100


//shared memory clock size
//like the previous project, we will use an array of two unsigned ints to represent the clock,
//where clock[0] is seconds and clock[1] is nanoseconds. This allows us to easily handle the clock math and normalization.
const size_t BUFF_SZ = sizeof(unsigned int) * 2;

struct Message {
    //msg structure used between OSS and worker.
    //In step 2, this only tested that messages worked.
    //In step 3, this now carries a memory request.

    long mtype;
    //mtype = message type
    //OSS sends messages using the worker pid as the message type.
    //WORKER sends messages back using message type 1.

    int value;
    //value is a simple status/control value.
    //OSS sends value 1 to tell the child it can take a turn.
    //WORKER sends value 1 to mean it is making a memory request.
    //OSS sends value 1 back to mean the memory request was granted.

    int pid;
    //the pid of the process sending the message.

    int slot;
    //the PCB slot associated with the worker.

    int address;
    //the logical memory address requested by the worker.
    //For this project, the child has addresses from 0 to 16383.
    //That is 16 pages times 1024 bytes per page.

    int requestType;
    //REQUEST_READ means this is a read request.
    //REQUEST_WRITE means this is a write request.
};

struct PCB {
    int occupied;
    pid_t pid;
    int localPid;
    int startSeconds;
    int startNano;
    int blocked;
    int pageTable[PAGE_COUNT];
};

struct Frame {
    //Frame table structure.
    //Assignment says total memory is 64K.
    //Page size is 1K.
    //That means we have 64 frames in the system.

    int occupied;
    int dirtyBit;
    int process;
    int page;
};

struct IORequest {
    int occupied;
    int slot;
    int pid;
    int address;
    int requestType;
    int page;
    long long readyTimeNS;
};

//global shared memory id.
//this is global so cleanupIPC can remove the shared memory segment even if the program is interrupted by a signal.
static int shm_id_global = -1;

//global msg queue id.
//This is global so cleanupIPC can remove the message queue during cleanup.
static int msg_id_global = -1;

//global pointer to the shared memory clock
//this is global so cleanupIPC can detach from it even if the program exits early.
static unsigned int *clock_global = NULL;

//global log file pointer.
//This lets logBoth write to the log file without needing a FILE pointer parameter every time.
static FILE *logFileGlobal = NULL;

static int logLineCount = 0;
static int logLimitMessagePrinted = 0;

//global PCB table and frame table.
struct PCB pcbTable[MAX_PCB_SIZE];
struct Frame frameTable[FRAME_COUNT];
struct IORequest ioQueue[MAX_IO_QUEUE];

int ioHead = 0;
int ioTail = 0;
int ioCount = 0;

long long getClockNS(unsigned int *clock);
void setClockFromNS(unsigned int *clock, long long timeNS);
void addToClock(unsigned int *clock, long long timeToAddNS);
long long secondsToNS(double seconds);

//print to screen and log file if open, with a line limit for the log file. not that necessary for task 1 but it's been shown the previous couple of projects 
// and will be useful for later tasks when there is more logging.
void logBoth(const char *format, ...) {
    va_list args;

    //first print to screen
    va_start(args, format);
    vprintf(format, args);
    va_end(args);

    fflush(stdout);

    //print same message to the log file if the log file was opened successfully.
    if (logFileGlobal != NULL) {
        if (logLineCount < MAX_LOG_LINES) {
            va_start(args, format);
            vfprintf(logFileGlobal, format, args);
            va_end(args);

            fflush(logFileGlobal);
            logLineCount++;
        } else if (!logLimitMessagePrinted) {
            fprintf(logFileGlobal,
                    "OSS: Log line limit reached. Further file logging stopped.\n");
            fflush(logFileGlobal);
            logLimitMessagePrinted = 1;
        }
    }
}

// Clear one PCB entry.
// This sets the slot back to unused and resets the page table to -1.
void clearPCBEntry(int slot) {
    int page;

    pcbTable[slot].occupied = 0;
    pcbTable[slot].pid = 0;
    pcbTable[slot].localPid = 0;
    pcbTable[slot].startSeconds = 0;
    pcbTable[slot].startNano = 0;
    pcbTable[slot].blocked = 0;

    //Every page table entry starts at -1.
    //-1 means that page is not currently loaded into a frame
    for (page = 0; page < PAGE_COUNT; page++) {
        pcbTable[slot].pageTable[page] = -1;
    }
}

// Initialize the full PCB table.
void initPCBTable() {
    int i;

    for (i = 0; i < MAX_PCB_SIZE; i++) {
        clearPCBEntry(i);
    }
}

// Initialize the frame table.
// Every frame starts empty.
void initFrameTable() {
    int i;

    for (i = 0; i < FRAME_COUNT; i++) {
        frameTable[i].occupied = 0;
        frameTable[i].dirtyBit = 0;
        frameTable[i].process = -1;
        frameTable[i].page = -1;
    }
}

//Find an unused PCB slot.
int findFreePCBSlot() {
    int i;

    for (i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied == 0) {
            return i;
        }
    }

    return -1;
}

//Find the next occupied PCB slot starting at startSlot.
//This gives OSS a simple round-robin style way to choose which worker to message.
int findNextOccupiedPCBSlot(int startSlot) {
    int i;

    for (i = 0; i < MAX_PCB_SIZE; i++) {
        int checkSlot = (startSlot + i) % MAX_PCB_SIZE;

        if (pcbTable[checkSlot].occupied == 1) {
            return checkSlot;
        }
    }

    return -1;
}

//Find an empty frame in the frame table.
//Returns the frame number if one is available.
//Returns -1 if every frame is occupied.
int findFreeFrame() {
    int i;

    for (i = 0; i < FRAME_COUNT; i++) {
        if (frameTable[i].occupied == 0) {
            return i;
        }
    }

    return -1;
}

//Choose a frame to evict for step 5.
int chooseSimpleVictimFrame() {
    int i;

    for (i = 0; i < FRAME_COUNT; i++) {
        if (frameTable[i].occupied == 1) {
            return i;
        }
    }

    return 0;
}

//Release all frames owned by a process.
//
//The assignment says that when a process terminates,
//OSS should release any frames used by that process.
//
//Here, frameTable[i].process stores the PCB slot number.
void releaseFramesForProcess(int slot) {
    int i;

    for (i = 0; i < FRAME_COUNT; i++) {
        if (frameTable[i].occupied == 1 && frameTable[i].process == slot) {
            logBoth("OSS: Releasing frame %d from P%d page %d\n",
                    i,
                    pcbTable[slot].localPid,
                    frameTable[i].page);

            frameTable[i].occupied = 0;
            frameTable[i].dirtyBit = 0;
            frameTable[i].process = -1;
            frameTable[i].page = -1;
        }
    }
}

//Load a process page into a frame.
//
//If there is a free frame, use it.
//If there is no free frame, evict a victim frame.
//
//Step 7 adds dirty bit cost.
//If the victim frame is dirty, OSS adds extra simulated time
//because the dirty page has to be written back before replacement.
int loadPageIntoFrame(int slot, int page, int requestType, unsigned int *clock) {
    int frame = findFreeFrame();

    //If no frame is free, choose a victim frame to clear.
    if (frame == -1) {
        frame = chooseSimpleVictimFrame();

        int oldProcess = frameTable[frame].process;
        int oldPage = frameTable[frame].page;
        int oldDirtyBit = frameTable[frame].dirtyBit;

        logBoth("OSS: No free frame available. Clearing frame %d from process slot %d page %d\n",
                frame,
                oldProcess,
                oldPage);

        //Step 7:
        //If the victim frame is dirty, it takes extra time to swap it out.
        //This simulates writing the dirty page back to disk before replacing it.
        if (oldDirtyBit == 1) {
            logBoth("OSS: Dirty bit of frame %d is set. Adding extra dirty swap time at %u:%u\n",
                    frame,
                    clock[0],
                    clock[1]);

            addToClock(clock, DIRTY_SWAP_EXTRA_NS);

            logBoth("OSS: Dirty frame %d write-back complete at %u:%u\n",
                    frame,
                    clock[0],
                    clock[1]);
        }

        //If the old frame belonged to a valid process/page,
        //mark that old process's page as no longer loaded.
        if (oldProcess >= 0 &&
            oldProcess < MAX_PCB_SIZE &&
            oldPage >= 0 &&
            oldPage < PAGE_COUNT) {
            pcbTable[oldProcess].pageTable[oldPage] = -1;

            logBoth("OSS: P%d page %d is now marked as not in memory\n",
                    pcbTable[oldProcess].localPid,
                    oldPage);
        }
    } else {
        logBoth("OSS: Found free frame %d\n", frame);
    }

    //Put the requested page into the chosen frame.
    frameTable[frame].occupied = 1;

    //Dirty bit starts at 0 when the page is first placed in the frame.
    frameTable[frame].dirtyBit = 0;

    frameTable[frame].process = slot;
    frameTable[frame].page = page;

    //Update the process page table so this page points to the frame.
    pcbTable[slot].pageTable[page] = frame;

    //If the current request is a write, the new frame becomes dirty.
    if (requestType == REQUEST_WRITE) {
        frameTable[frame].dirtyBit = 1;

        logBoth("OSS: Dirty bit of frame %d set because P%d is writing page %d\n",
                frame,
                pcbTable[slot].localPid,
                page);
    }

    logBoth("OSS: Loaded P%d page %d into frame %d\n",
            pcbTable[slot].localPid,
            page,
            frame);

    return frame;
}

//Initialize the I/O wait queue.
//This queue stores page fault requests that are waiting on disk.
void initIOQueue() {
    int i;

    for (i = 0; i < MAX_IO_QUEUE; i++) {
        ioQueue[i].occupied = 0;
        ioQueue[i].slot = -1;
        ioQueue[i].pid = 0;
        ioQueue[i].address = 0;
        ioQueue[i].requestType = REQUEST_READ;
        ioQueue[i].page = -1;
        ioQueue[i].readyTimeNS = 0;
    }

    ioHead = 0;
    ioTail = 0;
    ioCount = 0;
}

//Return true if the I/O queue is empty.
int isIOQueueEmpty() {
    return ioCount == 0;
}

//Return the time when the first blocked request will be ready.
long long getNextIOReadyTime() {
    if (ioCount <= 0) {
        return -1;
    }

    return ioQueue[ioHead].readyTimeNS;
}

//Add a page fault request to the I/O queue.
//The worker is blocked because OSS does not send a reply yet.
int enqueueIORequest(int slot,
                     int pid,
                     int address,
                     int requestType,
                     int page,
                     unsigned int *clock) {
    if (ioCount >= MAX_IO_QUEUE) {
        logBoth("OSS: I/O queue is full. Cannot queue page fault request.\n");
        return 0;
    }

    ioQueue[ioTail].occupied = 1;
    ioQueue[ioTail].slot = slot;
    ioQueue[ioTail].pid = pid;
    ioQueue[ioTail].address = address;
    ioQueue[ioTail].requestType = requestType;
    ioQueue[ioTail].page = page;

    //This request becomes ready after simulated disk time.
    ioQueue[ioTail].readyTimeNS = getClockNS(clock) + DISK_IO_NS;

    logBoth("OSS: Queued page fault for P%d address %d page %d. It will be ready at %lld ns\n",
            pcbTable[slot].localPid,
            address,
            page,
            ioQueue[ioTail].readyTimeNS);

    ioTail = (ioTail + 1) % MAX_IO_QUEUE;
    ioCount++;

    return 1;
}

//Remove the head request from the I/O queue.
struct IORequest dequeueIORequest() {
    struct IORequest request = ioQueue[ioHead];

    ioQueue[ioHead].occupied = 0;
    ioQueue[ioHead].slot = -1;
    ioQueue[ioHead].pid = 0;
    ioQueue[ioHead].address = 0;
    ioQueue[ioHead].requestType = REQUEST_READ;
    ioQueue[ioHead].page = -1;
    ioQueue[ioHead].readyTimeNS = 0;

    ioHead = (ioHead + 1) % MAX_IO_QUEUE;
    ioCount--;

    return request;
}

//Find the next occupied PCB slot that is not blocked.
//Blocked processes should not receive another turn message.
int findNextUnblockedPCBSlot(int startSlot) {
    int i;

    for (i = 0; i < MAX_PCB_SIZE; i++) {
        int checkSlot = (startSlot + i) % MAX_PCB_SIZE;

        if (pcbTable[checkSlot].occupied == 1 &&
            pcbTable[checkSlot].blocked == 0) {
            return checkSlot;
        }
    }

    return -1;
}

//Count active unblocked processes.
//This helps OSS detect when every active worker is blocked.
int countUnblockedProcesses() {
    int i;
    int count = 0;

    for (i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied == 1 && pcbTable[i].blocked == 0) {
            count++;
        }
    }

    return count;
}

//Send a grant message to a worker.
//This is used both for normal memory hits and for completed I/O page faults.
int sendGrantMessage(int msg_id,
                     int pid,
                     int slot,
                     int address,
                     int requestType) {
    struct Message grantMessage;

    grantMessage.mtype = pid;
    grantMessage.value = MSG_GRANTED;
    grantMessage.pid = getpid();
    grantMessage.slot = slot;
    grantMessage.address = address;
    grantMessage.requestType = requestType;

    if (msgsnd(msg_id, &grantMessage, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: Error in msgsnd grant message");
        return 0;
    }

    return 1;
}

//Check the I/O wait queue.
//If the request at the head is ready, load the page and grant the request.
void checkIOQueue(int msg_id, unsigned int *clock) {
    int keepChecking = 1;

    while (keepChecking && ioCount > 0) {
        struct IORequest request = ioQueue[ioHead];

        long long currentNS = getClockNS(clock);

        if (currentNS < request.readyTimeNS) {
            keepChecking = 0;
        } else {
            request = dequeueIORequest();

            if (request.slot < 0 ||
                request.slot >= MAX_PCB_SIZE ||
                pcbTable[request.slot].occupied == 0) {
                logBoth("OSS: I/O request completed, but process is gone. Dropping request.\n");
                continue;
            }

            logBoth("OSS: I/O complete for P%d address %d page %d at time %u:%u\n",
                    pcbTable[request.slot].localPid,
                    request.address,
                    request.page,
                    clock[0],
                    clock[1]);

            int frame = loadPageIntoFrame(request.slot,
                                        request.page,
                                        request.requestType,
                                        clock);

            if (request.requestType == REQUEST_WRITE) {
                frameTable[frame].dirtyBit = 1;
            }

            pcbTable[request.slot].blocked = 0;

            logBoth("OSS: Unblocking P%d. Address %d is now in frame %d\n",
                    pcbTable[request.slot].localPid,
                    request.address,
                    frame);

            sendGrantMessage(msg_id,
                             request.pid,
                             request.slot,
                             request.address,
                             request.requestType);
        }
    }
}

// Print the current process table.
void printProcessTable(unsigned int *clock) {
    int i;

    logBoth("\nOSS PID:%d SysClockS: %u SysClockNano: %u\n",
            getpid(),
            clock[0],
            clock[1]);

    logBoth("Process Table:\n");
    logBoth("Entry Occupied LocalPID RealPID StartS StartN Blocked\n");

    for (i = 0; i < MAX_PCB_SIZE; i++) {
        logBoth("%5d %8d %8d %7d %6d %6d %7d\n",
                i,
                pcbTable[i].occupied,
                pcbTable[i].localPid,
                (int)pcbTable[i].pid,
                pcbTable[i].startSeconds,
                pcbTable[i].startNano,
                pcbTable[i].blocked);
    }
}

// Print the page table for any occupied PCB slot.
void printPageTables() {
    int slot;
    int page;

    logBoth("\nPage Tables:\n");

    for (slot = 0; slot < MAX_PCB_SIZE; slot++) {
        if (pcbTable[slot].occupied == 1) {
            logBoth("P%d page table: [ ", pcbTable[slot].localPid);

            for (page = 0; page < PAGE_COUNT; page++) {
                logBoth("%d ", pcbTable[slot].pageTable[page]);
            }

            logBoth("]\n");
        }
    }
}

// Print the frame table.
// For Step 2, all frames should still be empty.
void printFrameTable() {
    int i;

    logBoth("\nFrame Table:\n");
    logBoth("Frame Occupied DirtyBit Process Page\n");

    for (i = 0; i < FRAME_COUNT; i++) {
        logBoth("%5d %8d %8d %7d %4d\n",
                i,
                frameTable[i].occupied,
                frameTable[i].dirtyBit,
                frameTable[i].process,
                frameTable[i].page);
    }
}

//cleanup all IPC created by OSS.
//In step 4, OSS creates two IPC resources:
//
//1. shared memory
//2. message queue
//
//OSS is responsible for removing both before the program ends.
void cleanupIPC() {
    //detach from shared memory if OSS is currently attached.
    if (clock_global != NULL && clock_global != (void *) -1) {
        if (shmdt(clock_global) == -1) {
            perror("OSS: Error in shmdt");
        }

        clock_global = NULL;
    }

    //remove the shared memory segment if it was created.
    if (shm_id_global != -1) {
        if (shmctl(shm_id_global, IPC_RMID, NULL) == -1) {
            perror("OSS: Error in shmctl");
        }

        shm_id_global = -1;
    }

    //remove the message queue if it was created.
    if (msg_id_global != -1) {
        if (msgctl(msg_id_global, IPC_RMID, NULL) == -1) {
            perror("OSS: Error in msgctl");
        }

        msg_id_global = -1;
    }
}

// Kill any child process still recorded in the PCB table.
void killRunningChildren() {
    int i;

    for (i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied == 1 && pcbTable[i].pid > 0) {
            kill(pcbTable[i].pid, SIGTERM);
        }
    }
}

//lets OSS clean up if the user presses Ctrl+C or the program receives a termination signal.
//This matters because OSS is creating IPC resources.
//If OSS dies without cleanup, shared memory or message queues can be left behind.
//
//For step 2, there should only be one worker.
//Later, there will be many workers.
//So this signal handler already uses killRunningChildren to clean up anything in the PCB table.
void signal_handler(int sig) {
    logBoth("OSS: received signal %d, shutting down...\n", sig);

    //send SIGTERM to any child process recorded in the PCB table.
    killRunningChildren();

    //wait for any children that are still around.
    //This prevents zombie processes.
    while (wait(NULL) > 0) {
    }

    //remove shared memory and the message queue.
    cleanupIPC();

    //close the log file if it is open.
    if (logFileGlobal != NULL) {
        fclose(logFileGlobal);
        logFileGlobal = NULL;
    }

    exit(1);
}

 //convert shm clock to nano seconds for easier math
 //clock 0 = seconds 1 = nano 
long long getClockNS(unsigned int *clock) {
    return ((long long)clock[0] * NANOPERSEC) + clock[1];
}

 //set shm clock using a total number of nanoseconds
 //breaks the total nano seconds back into seconds and nano for clock array
void setClockFromNS(unsigned int *clock, long long timeNS) {
    if (timeNS < 0) {
        timeNS = 0;
    }

    clock[0] = (unsigned int)(timeNS / NANOPERSEC);
    clock[1] = (unsigned int)(timeNS % NANOPERSEC);
}


 //add nanosec to simulated system clock
 //keeps the clock normalized so that nanoseconds never stays above 1 billion which is just equal to 1 second. 
void addToClock(unsigned int *clock, long long timeToAddNS) {
    long long currentTimeNS = getClockNS(clock);
    long long newTimeNS = currentTimeNS + timeToAddNS;

    setClockFromNS(clock, newTimeNS);
}

// Convert seconds from the command line into nanoseconds.
long long secondsToNS(double seconds) {
    if (seconds <= 0) {
        return 0;
    }

    return (long long)(seconds * NANOPERSEC);
}

//Launch one worker process.
//This is new for step 4 because we need to launch multiple workers.
int launchChildProcess(float t,
                       unsigned int *clock,
                       int *launchedChildren,
                       int *activeChildren) {
    //Find a free PCB slot for this new worker.
    int slot = findFreePCBSlot();

    if (slot == -1) {
        logBoth("OSS: No free PCB slot available.\n");
        return 0;
    }

    //Convert the child time limit from seconds into total nanoseconds.
    long long timeLimitNS = secondsToNS(t);

    //Make sure the time limit is positive.
    if (timeLimitNS <= 0) {
        timeLimitNS = NANOPERSEC;
    }

    //Split total nanoseconds into seconds and nanoseconds.
    int timeLimitSec = (int)(timeLimitNS / NANOPERSEC);
    int timeLimitNano = (int)(timeLimitNS % NANOPERSEC);

    //These strings will be passed into execl.
    //execl passes arguments as strings.
    char secStr[32];
    char nanoStr[32];

    snprintf(secStr, sizeof(secStr), "%d", timeLimitSec);
    snprintf(nanoStr, sizeof(nanoStr), "%d", timeLimitNano);

    //Fill in the PCB entry before forking.
    //The real pid is filled in after fork succeeds.
    pcbTable[slot].occupied = 1;
    pcbTable[slot].pid = 0;
    pcbTable[slot].localPid = (*launchedChildren) + 1;
    pcbTable[slot].startSeconds = clock[0];
    pcbTable[slot].startNano = clock[1];
    pcbTable[slot].blocked = 0;

    //fork creates the child process.
    pid_t pid = fork();

    if (pid == -1) {
        perror("OSS: Error in fork");

        clearPCBEntry(slot);

        return 0;
    }

    //Child process section.
    //The child becomes worker.
    if (pid == 0) {
        execl("./worker", "worker", secStr, nanoStr, (char *) NULL);

        perror("OSS: Error in execl");
        exit(EXIT_FAILURE);
    }

    //Parent process section.
    //Store the worker's real pid in the PCB table.
    pcbTable[slot].pid = pid;

    (*launchedChildren)++;
    (*activeChildren)++;

    logBoth("\nOSS: Forked worker P%d with PID %d in slot %d at time %u:%u\n",
            pcbTable[slot].localPid,
            (int)pcbTable[slot].pid,
            slot,
            clock[0],
            clock[1]);

    return 1;
}

//Send one turn message to a worker and receive its reply.
//
//If the worker says it is done, clean it up.
//If the worker requests a memory address already in memory, grant it.
//If the worker requests a page not in memory, block it and queue the page fault.
//
//In Step 6, page faults are no longer granted immediately.
int handleWorkerMemoryRequest(int msg_id,
                              int slot,
                              unsigned int *clock,
                              int *activeChildren,
                              int *finishedChildren,
                              int *totalRequests,
                              int *totalReads,
                              int *totalWrites,
                              int *totalPageFaults) {
    int status;

    pid_t pid = pcbTable[slot].pid;

    //Increment the clock before sending the worker a message.
    addToClock(clock, TURN_INCREMENT_NS);

    //Create the turn message for the worker.
    struct Message msgToChild;

    msgToChild.mtype = pid;
    msgToChild.value = MSG_MEMORY_REQUEST;
    msgToChild.pid = getpid();
    msgToChild.slot = slot;
    msgToChild.address = 0;
    msgToChild.requestType = REQUEST_READ;

    logBoth("OSS: Sending turn message to worker P%d PID %d at time %u:%u\n",
            pcbTable[slot].localPid,
            (int)pid,
            clock[0],
            clock[1]);

    if (msgsnd(msg_id, &msgToChild, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: Error in msgsnd");

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        releaseFramesForProcess(slot);
        clearPCBEntry(slot);

        (*activeChildren)--;

        return 0;
    }

    //Receive either a memory request or a termination message.
    struct Message msgFromChild;

    if (msgrcv(msg_id, &msgFromChild, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
        perror("OSS: Error in msgrcv");

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        releaseFramesForProcess(slot);
        clearPCBEntry(slot);

        (*activeChildren)--;

        return 0;
    }

    int requestSlot = msgFromChild.slot;

    if (requestSlot < 0 || requestSlot >= MAX_PCB_SIZE) {
        requestSlot = slot;
    }

    if (pcbTable[requestSlot].occupied == 0) {
        requestSlot = slot;
    }

    //If value is 0, the worker says its time is up.
    if (msgFromChild.value == MSG_TERMINATE) {
        logBoth("OSS: Worker P%d PID %d says it is terminating at time %u:%u\n",
                pcbTable[requestSlot].localPid,
                (int)pid,
                clock[0],
                clock[1]);

        waitpid(pid, &status, 0);

        releaseFramesForProcess(requestSlot);
        clearPCBEntry(requestSlot);

        (*activeChildren)--;
        (*finishedChildren)++;

        return 1;
    }

    const char *requestString = "read";

    if (msgFromChild.requestType == REQUEST_WRITE) {
        requestString = "write";
    }

    int requestedPage = msgFromChild.address / PAGE_SIZE;
    int requestedOffset = msgFromChild.address % PAGE_SIZE;

    if (requestedPage < 0 || requestedPage >= PAGE_COUNT) {
        logBoth("OSS: Invalid page %d from address %d. Killing worker P%d\n",
                requestedPage,
                msgFromChild.address,
                pcbTable[requestSlot].localPid);

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        releaseFramesForProcess(requestSlot);
        clearPCBEntry(requestSlot);

        (*activeChildren)--;

        return 0;
    }

    (*totalRequests)++;

    if (msgFromChild.requestType == REQUEST_WRITE) {
        (*totalWrites)++;
    } else {
        (*totalReads)++;
    }

    logBoth("OSS: P%d requesting %s of address %d at time %u:%u\n",
            pcbTable[requestSlot].localPid,
            requestString,
            msgFromChild.address,
            clock[0],
            clock[1]);

    logBoth("OSS: Address %d is page %d with offset %d\n",
            msgFromChild.address,
            requestedPage,
            requestedOffset);

    int frame = pcbTable[requestSlot].pageTable[requestedPage];

    if (frame == -1) {
        //Page fault.
        //Step 6 queues this request and does not send a message back yet.
        (*totalPageFaults)++;

        logBoth("OSS: Address %d is not in a frame, pagefault\n",
                msgFromChild.address);

        logBoth("OSS: Blocking P%d while page %d is brought in from disk\n",
                pcbTable[requestSlot].localPid,
                requestedPage);

        pcbTable[requestSlot].blocked = 1;

        if (!enqueueIORequest(requestSlot,
                              msgFromChild.pid,
                              msgFromChild.address,
                              msgFromChild.requestType,
                              requestedPage,
                              clock)) {
            logBoth("OSS: Could not queue I/O request. Killing P%d\n",
                    pcbTable[requestSlot].localPid);

            kill(pid, SIGTERM);
            waitpid(pid, NULL, 0);

            releaseFramesForProcess(requestSlot);
            clearPCBEntry(requestSlot);

            (*activeChildren)--;

            return 0;
        }

        //No grant is sent here.
        //The worker remains blocked on msgrcv until checkIOQueue sends the grant later.
        return 1;
    }

    //No page fault.
    //The page is already loaded, so grant immediately.
    logBoth("OSS: Address %d is already in frame %d\n",
            msgFromChild.address,
            frame);

    if (msgFromChild.requestType == REQUEST_WRITE) {
        frameTable[frame].dirtyBit = 1;

        logBoth("OSS: Dirty bit of frame %d set because this was a write\n",
                frame);
    }

    addToClock(clock, MEMORY_ACCESS_NS);

    if (msgFromChild.requestType == REQUEST_WRITE) {
        logBoth("OSS: Address %d in frame %d, writing data for P%d at time %u:%u\n",
                msgFromChild.address,
                frame,
                pcbTable[requestSlot].localPid,
                clock[0],
                clock[1]);
    } else {
        logBoth("OSS: Address %d in frame %d, giving data to P%d at time %u:%u\n",
                msgFromChild.address,
                frame,
                pcbTable[requestSlot].localPid,
                clock[0],
                clock[1]);
    }

    if (!sendGrantMessage(msg_id,
                          msgFromChild.pid,
                          requestSlot,
                          msgFromChild.address,
                          msgFromChild.requestType)) {
        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        releaseFramesForProcess(requestSlot);
        clearPCBEntry(requestSlot);

        (*activeChildren)--;

        return 0;
    }

    return 1;
}

void printUsage(char *programName) {
    printf("Usage: %s [-h] [-n proc] [-s simul] "
           "[-t timeLimitForChildren] "
           "[-i fractionOfSecondToLaunchChildren] [-f logfile]\n",
           programName);
}

int main(int argc, char *argv[]) {
    //default values for command line options
    int n = 1;
    int s = 1;
    float t = 1.0f;
    float i = 0.0f;
    char logFile[256] = "log.txt";

    int opt;

    //parse command line options using getopt
    while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
        switch (opt) {
            case 'h':
                printUsage(argv[0]);
                return EXIT_SUCCESS;

            case 'n':
                n = atoi(optarg);
                break;

            case 's':
                s = atoi(optarg);
                break;

            case 't':
                t = atof(optarg);
                break;

            case 'i':
                i = atof(optarg);
                break;

            case 'f':
                strncpy(logFile, optarg, sizeof(logFile) - 1);
                logFile[sizeof(logFile) - 1] = '\0';
                break;

            default:
                printUsage(argv[0]);
                return EXIT_FAILURE;
        }
    }

    //avoid invalid values for command line options
    if (n <= 0) {
        n = 1;
    }

    if (s <= 0) {
        s = 1;
    }

    if (t <= 0) {
        t = 1.0f;
    }

    if (i < 0) {
        i = 0.0f;
    }

    if (s > n) {
        s = n;
    }

    if (s > MAX_PCB_SIZE) {
        s = MAX_PCB_SIZE;
    }

    //open log file for writing and save the file pointer globally.
    logFileGlobal = fopen(logFile, "w");

    if (logFileGlobal == NULL) {
        perror("OSS: Error opening log file");
        return EXIT_FAILURE;
    }

    //set up signal handlers after the logfile is open.
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    initPCBTable();
    initFrameTable();
    initIOQueue();

    logBoth("OSS: starting, PID:%d PPID:%d\n", getpid(), getppid());
    logBoth("OSS called with:\n");
    logBoth("-n %d\n", n);
    logBoth("-s %d\n", s);
    logBoth("-t %.3f\n", t);
    logBoth("-i %.3f\n", i);
    logBoth("-f %s\n", logFile);

    //create a key for shared memory using ftok.
    key_t shm_key = ftok("oss.c", 0);

    if (shm_key == (key_t) -1) {
        perror("OSS: Error in ftok for shared memory");

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //create the shm segment with the generated key, size of the clock, and permissions.
    int shm_id = shmget(shm_key, BUFF_SZ, IPC_CREAT | 0700);

    if (shm_id == -1) {
        perror("OSS: Error in shmget");

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //save the shared memory id globally so cleanupIPC can remove it.
    shm_id_global = shm_id;

    //attach OSS to the shared memory segment.
    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);

    if (clock == (void *) -1) {
        perror("OSS: Error in shmat");

        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //save the shm clock pointer globally so cleanupIPC can detach from it if needed.
    clock_global = clock;

    //initialize the simulated clock to 0 seconds and 0 nanoseconds.
    clock[0] = 0;
    clock[1] = 0;

    logBoth("OSS: Shared memory clock initialized.\n");
    logBoth("OSS: Initial clock is %u:%u\n", clock[0], clock[1]);

    //create a message queue key using ftok.
    key_t msg_key = ftok("oss.c", 1);

    if (msg_key == (key_t) -1) {
        perror("OSS: Error in ftok for message queue");

        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //create the message queue.
    int msg_id = msgget(msg_key, IPC_CREAT | 0700);

    if (msg_id == -1) {
        perror("OSS: Error in msgget");

        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //save the message queue id globally.
    msg_id_global = msg_id;

    logBoth("OSS: Message queue created with id %d\n", msg_id);

    //print the starting tables before any child is created.
    printProcessTable(clock);
    printPageTables();
    printFrameTable();

    //Step 4 variables.
    int launchedChildren = 0;
    int activeChildren = 0;
    int finishedChildren = 0;

    int totalRequests = 0;
    int totalReads = 0;
    int totalWrites = 0;
    int totalPageFaults = 0;

    //This is used to choose which occupied PCB slot gets messaged next.
    int nextMessageSlot = 0;

    //Convert the launch interval from seconds into nanoseconds.
    //If -i is 0, OSS can launch children as soon as slots are available.
    long long launchIntervalNS = secondsToNS(i);

    //The first child can launch at simulated time 0.
    long long nextLaunchNS = 0;

    //Main step 4 loop.
    //
    //Keep looping while:
    //- there are still children left to launch, or
    //- there are active children still in the system.
    while (launchedChildren < n || activeChildren > 0) {
        long long currentNS = getClockNS(clock);
        checkIOQueue(msg_id, clock);

        
        //Refresh currentNS in case checkIOQueue changed anything important.
        currentNS = getClockNS(clock);
        
        //Launch as many children as allowed.
        //
        //Conditions:
        //- do not launch more than n total children
        //- do not exceed s simultaneous children
        //- do not launch before the next launch time
        while (launchedChildren < n &&
               activeChildren < s &&
               currentNS >= nextLaunchNS) {
            if (!launchChildProcess(t, clock, &launchedChildren, &activeChildren)) {
                logBoth("OSS: Error launching child. Beginning cleanup.\n");

                killRunningChildren();

                while (wait(NULL) > 0) {
                }

                cleanupIPC();

                fclose(logFileGlobal);
                logFileGlobal = NULL;

                return EXIT_FAILURE;
            }

            //Update current time after launching.
            currentNS = getClockNS(clock);

            //If the user gave a positive -i value, schedule the next launch.
            //Then break so we do not launch another child until that time.
            if (launchIntervalNS > 0) {
                nextLaunchNS = currentNS + launchIntervalNS;
                break;
            }

            //If -i is 0, the next launch can happen immediately.
            nextLaunchNS = currentNS;
        }

        //Find the next active worker to message.
        int slotToMessage = findNextUnblockedPCBSlot(nextMessageSlot);
        
        if (slotToMessage != -1) {
            //Advance the round-robin starting point.
            nextMessageSlot = (slotToMessage + 1) % MAX_PCB_SIZE;

            //Send a turn, receive one memory request, always grant it,
            //then wait for the worker to terminate.
                handleWorkerMemoryRequest(msg_id,
                                        slotToMessage,
                                        clock,
                                        &activeChildren,
                                        &finishedChildren,
                                        &totalRequests,
                                        &totalReads,
                                        &totalWrites,
                                        &totalPageFaults);
        } else {
            //No unblocked worker can run right now.
            //This can happen when every active process is blocked on I/O.

            if (activeChildren > 0 && countUnblockedProcesses() == 0 && !isIOQueueEmpty()) {
                long long nextReadyNS = getNextIOReadyTime();
                long long currentNS = getClockNS(clock);

                if (nextReadyNS > currentNS) {
                    logBoth("OSS: All active processes are blocked. Advancing clock to next I/O completion.\n");
                    setClockFromNS(clock, nextReadyNS);
                }

                checkIOQueue(msg_id, clock);
            } else if (launchedChildren < n) {
                currentNS = getClockNS(clock);

                if (currentNS < nextLaunchNS) {
                    setClockFromNS(clock, nextLaunchNS);
                } else {
                    addToClock(clock, IDLE_INCREMENT_NS);
                }
            }
        }
    }

    //Print final tables.
    printProcessTable(clock);
    printPageTables();
    printFrameTable();

    logBoth("\nOSS: Step 7 Summary\n");
    logBoth("OSS: Children launched: %d\n", launchedChildren);
    logBoth("OSS: Children finished: %d\n", finishedChildren);
    logBoth("OSS: Total memory requests: %d\n", totalRequests);
    logBoth("OSS: Total reads: %d\n", totalReads);
    logBoth("OSS: Total writes: %d\n", totalWrites);
    logBoth("OSS: Total page faults: %d\n", totalPageFaults);
    logBoth("OSS: Final clock: %u:%u\n", clock[0], clock[1]);

    //clean up shared memory and the message queue.
    cleanupIPC();

    logBoth("OSS: Shared memory and message queue cleaned up.\n");

    //close logfile at the end.
    if (logFileGlobal != NULL) {
        fclose(logFileGlobal);
        logFileGlobal = NULL;
    }

    return EXIT_SUCCESS;
}