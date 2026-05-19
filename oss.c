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
#define TURN_INCREMENT_NS 1000LL

//This is how much time OSS adds when it grants a memory request.
//Step 3 says normal memory access is granted immediately.
//Later, page faults will take much longer.
#define MEMORY_ACCESS_NS 100LL

//This is used when OSS has no active process to message but still needs time to move.
#define IDLE_INCREMENT_NS 10000LL

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

//Send one turn message to a worker, receive its memory request,
//always grant it, then wait for that worker to terminate.
//
//This function is basically the step 3 message logic moved into a reusable function.
//Step 4 needs this because we now repeat the same process for many workers.
int handleWorkerMemoryRequest(int msg_id,
                              int slot,
                              unsigned int *clock,
                              int *activeChildren,
                              int *finishedChildren,
                              int *totalRequests,
                              int *totalReads,
                              int *totalWrites) {
    int status;

    pid_t pid = pcbTable[slot].pid;

    //Increment the clock before sending the worker a message.
    addToClock(clock, TURN_INCREMENT_NS);

    //Create a message that OSS will send to the worker.
    struct Message msgToChild;

    //The worker receives messages where mtype equals the worker's real pid.
    msgToChild.mtype = pid;

    //value = 1 means the worker is allowed to take a turn.
    msgToChild.value = 1;

    //OSS is the sender of this message.
    msgToChild.pid = getpid();

    //Tell the worker which PCB slot it belongs to.
    msgToChild.slot = slot;

    //These fields are not used in the turn message.
    //They are set to safe default values anyway.
    msgToChild.address = 0;
    msgToChild.requestType = REQUEST_READ;

    logBoth("OSS: Sending turn message to worker P%d PID %d at time %u:%u\n",
            pcbTable[slot].localPid,
            (int)pid,
            clock[0],
            clock[1]);

    //Send the turn message to the worker.
    if (msgsnd(msg_id, &msgToChild, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: Error in msgsnd");

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        clearPCBEntry(slot);

        (*activeChildren)--;

        return 0;
    }

    //Create a message variable for the worker's memory request.
    struct Message msgFromChild;

    //Wait for the worker to send back its memory request.
    //The worker sends messages back to OSS using mtype = 1.
    if (msgrcv(msg_id, &msgFromChild, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
        perror("OSS: Error in msgrcv");

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        clearPCBEntry(slot);

        (*activeChildren)--;

        return 0;
    }

    //If the slot sent by the worker looks wrong, use the slot OSS was already handling.
    //This is just a safety check for step 4.
    int requestSlot = msgFromChild.slot;

    if (requestSlot < 0 || requestSlot >= MAX_PCB_SIZE) {
        requestSlot = slot;
    }

    if (pcbTable[requestSlot].occupied == 0) {
        requestSlot = slot;
    }

    //Figure out whether the request was a read or a write.
    const char *requestString = "read";

    if (msgFromChild.requestType == REQUEST_WRITE) {
        requestString = "write";
    }

    //Extract the page number from the requested address.
    int requestedPage = msgFromChild.address / PAGE_SIZE;

    //Extract the offset too.
    int requestedOffset = msgFromChild.address % PAGE_SIZE;

    //Update simple step 4 statistics.
    (*totalRequests)++;

    if (msgFromChild.requestType == REQUEST_WRITE) {
        (*totalWrites)++;
    } else {
        (*totalReads)++;
    }

    //Log the memory request from the worker.
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

    //Step 4 still says to always grant the memory request.
    //So we do not check for page faults yet.
    //We do not fill the page table yet.
    //We do not fill the frame table yet.
    addToClock(clock, MEMORY_ACCESS_NS);

    if (msgFromChild.requestType == REQUEST_WRITE) {
        logBoth("OSS: Granting write request for address %d to P%d at time %u:%u\n",
                msgFromChild.address,
                pcbTable[requestSlot].localPid,
                clock[0],
                clock[1]);
    } else {
        logBoth("OSS: Granting read request for address %d to P%d at time %u:%u\n",
                msgFromChild.address,
                pcbTable[requestSlot].localPid,
                clock[0],
                clock[1]);
    }

    //Now send a grant message back to the worker.
    struct Message grantMessage;

    //Send the grant to the worker's pid.
    grantMessage.mtype = msgFromChild.pid;

    //value = 1 means the request was granted.
    grantMessage.value = 1;

    //OSS is the sender.
    grantMessage.pid = getpid();

    //Keep the same PCB slot.
    grantMessage.slot = requestSlot;

    //Send the same address back so the worker can verify what was granted.
    grantMessage.address = msgFromChild.address;

    //Send the same request type back.
    grantMessage.requestType = msgFromChild.requestType;

    if (msgsnd(msg_id, &grantMessage, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: Error in msgsnd grant message");

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        clearPCBEntry(slot);

        (*activeChildren)--;

        return 0;
    }

    //Wait for the child process to terminate.
    //The current worker exits after one granted memory request.
    if (waitpid(pid, &status, 0) == -1) {
        perror("OSS: Error in waitpid");

        clearPCBEntry(slot);

        (*activeChildren)--;

        return 0;
    }

    logBoth("OSS: Worker P%d PID %d has terminated.\n",
            pcbTable[slot].localPid,
            (int)pid);

    //Clear the PCB slot now that the worker is finished.
    clearPCBEntry(slot);

    (*activeChildren)--;
    (*finishedChildren)++;

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
        int slotToMessage = findNextOccupiedPCBSlot(nextMessageSlot);

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
                                      &totalWrites);
        } else {
            //No active process is ready to message.
            //If there are still children left to launch, move time toward the next launch.
            if (launchedChildren < n) {
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

    //Print a small step 4 summary.
    logBoth("\nOSS: Step 4 Summary\n");
    logBoth("OSS: Children launched: %d\n", launchedChildren);
    logBoth("OSS: Children finished: %d\n", finishedChildren);
    logBoth("OSS: Total memory requests: %d\n", totalRequests);
    logBoth("OSS: Total reads: %d\n", totalReads);
    logBoth("OSS: Total writes: %d\n", totalWrites);
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