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
    //Frame table structure
    //Assignment says total memory is 64K
    //Page size is 1k, so that means we have 64 frames in the system.

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

static FILE *logFileGlobal = NULL;
static int logLineCount = 0;
static int logLimitMessagePrinted = 0;

//print to screen and log file if open, with a line limit for the log file. not that necessary for task 1 but it's been shown the previous couple of projects 
// and will be useful for later tasks when there is more logging.

//global PCB table and frame table.

struct PCB pcbTable[MAX_PCB_SIZE];
struct Frame frameTable[FRAME_COUNT];

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
//In step 2, OSS creates two IPC resources:
//
//1. shared memory
//2. message queue
//
//OSS is responsible for removing both before the program ends.
//The worker should only detach from shared memory.
//The worker should not remove shared memory or the message queue.
void cleanupIPC() {
    //detach from shared memory if OSS is currently attached.
    //clock_global starts as NULL.
    //After shmat succeeds, clock_global points to the shared memory clock.
    //If shmat fails, it returns (void *) -1.
    //So we only call shmdt if clock_global is not NULL and not the shmat error value.
    if (clock_global != NULL && clock_global != (void *) -1) {
        if (shmdt(clock_global) == -1) {
            perror("OSS: Error in shmdt");
        }

        //set the pointer back to NULL so we do not accidentally detach twice.
        clock_global = NULL;
    }

    //remove the shared memory segment if it was created.
    //IPC_RMID marks the shared memory segment for deletion.
    //This prevents shared memory from being left behind after OSS exits.
    if (shm_id_global != -1) {
        if (shmctl(shm_id_global, IPC_RMID, NULL) == -1) {
            perror("OSS: Error in shmctl");
        }

        //set the id back to -1 so we know it is no longer valid.
        shm_id_global = -1;
    }

    //remove the message queue if it was created.
    //This is new for step 2.
    //If we forget this, ipcs may show a leftover message queue after the program ends.
    if (msg_id_global != -1) {
        if (msgctl(msg_id_global, IPC_RMID, NULL) == -1) {
            perror("OSS: Error in msgctl");
        }

        //set the id back to -1 so we know it is no longer valid.
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
    int status;

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

    //open log file for writing and save the file pointer globally so signal handler can close it on shutdown.
    logFileGlobal = fopen(logFile, "w");

    if (logFileGlobal == NULL) {
        perror("OSS: Error opening log file");
        return EXIT_FAILURE;
    }

     //set up signal handlers after the logfile is open so the signal handler can log the shutdown message
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

     //create a key for shared memory using ftok
    key_t shm_key = ftok("oss.c", 0);

    if (shm_key == (key_t) -1) {
        fprintf(stderr, "OSS: Error in ftok for shared memory\n");

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

     //create the shm segment with the generated key, size of the clock, and permissions.
     //what this does for us is it creates a shared memory segment in the system that can be accessed by multiple processes.
    int shm_id = shmget(shm_key, BUFF_SZ, IPC_CREAT | 0700);

    if (shm_id == -1) {
        fprintf(stderr, "OSS: Error in shmget\n");

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

     //save the shared memory id globally so cleanupIPC can remove it.
    shm_id_global = shm_id;

    //attach OSS to the shared memory segment.
    //shmat returns a pointer to the shared memory 
    //since the shared memory stores two unsigned ints, we treat it as an unsigned int pointer.
    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);

    if (clock == (void *) -1) {
        fprintf(stderr, "OSS: Error in shmat\n");

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
    //
    //This is similar to how we created the shared memory key.
    //The difference is that we use project id 1 instead of 0.
    //
    //ftok("oss.c", 0) was used for shared memory.
    //ftok("oss.c", 1) is used for the message queue.
    //
    //Using a different project id helps create a different IPC key.
    key_t msg_key = ftok("oss.c", 1);

    //check if ftok failed.
    //ftok returns (key_t) -1 on error.
    if (msg_key == (key_t) -1) {
        fprintf(stderr, "OSS: Error in ftok for message queue\n");

        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //create the message queue.
    //
    //msgget arguments:
    //
    //msg_key:
    //      The key created by ftok.
    //
    //IPC_CREAT | 0700:
    //      IPC_CREAT means create the queue if it does not exist.
    //      0700 gives the owner permission to use it.
    //
    //msgget returns the message queue id on success.
    //msgget returns -1 on failure.

    int msg_id = msgget(msg_key, IPC_CREAT | 0700);

    //check if msgget failed.
    if (msg_id == -1) {
        perror("OSS: Error in msgget");

        //shared memory was already created,
        //so remove it before leaving.
        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //save the message queue id globally.
    //This lets cleanupIPC remove the message queue later.
    msg_id_global = msg_id;

    logBoth("OSS: Message queue created with id %d\n", msg_id);

    //print the starting tables before any child is created.
    //
    //At this point:
    //- The PCB table should be empty.
    //- The page tables should all be empty.
    //- The frame table should show all frames as unused.
    printProcessTable(clock);
    printPageTables();
    printFrameTable();

    //step 2 only forks one worker
    //later steps will use n and s to launch more workers
    //for now, we use slot 0 so we can prove the basic fork/exec/message flow works.
    int slot = 0;
    int localPid = 1;

    //convert the child time limit from seconds into total nanoseconds.
    //
    //The command line option -t is a float.
    //For example:
    //
    //-t 1.5 means 1.5 seconds.
    //
    //The worker expects the time limit as two separate arguments:
    //seconds and nanoseconds.
    long long timeLimitNS = secondsToNS(t);

    //make sure the time limit is positive.
    //If somehow it is zero or negative, use 1 second as a safe default.
    if(timeLimitNS <= 0) {
        timeLimitNS = NANOPERSEC;
    }

    //split total nanoseconds into seconds and nanoseconds.
    //
    //Example:
    //1500000000 nanoseconds becomes:
    //1 second and 500000000 nanoseconds.
    int timeLimitSec = (int)(timeLimitNS / NANOPERSEC);
    int timeLimitNano = (int)(timeLimitNS % NANOPERSEC);

    
    //These strings will be passed into execl.
    //execl passes arguments as strings, just like command line arguments.
    //So we convert the integer time limit values into strings.
    char secStr[32];
    char nanoStr[32];

    //snprintf is used instead of sprintf because snprintf limits
    //how many characters can be written into the array.
    snprintf(secStr, sizeof(secStr), "%d", timeLimitSec);
    snprintf(nanoStr, sizeof(nanoStr), "%d", timeLimitNano);

    //fill in the PCB entry before forking
    //occupid = 1 means this PCB slot is being used
    //localPid is our simulated process number
    //startSeconds and startNano store when the process was created
    //blocked = 0 means this worker is not waiting on I/O yet.
    //actual real PID cannot be stored until after fork succeeds
    pcbTable[slot].occupied = 1;
    pcbTable[slot].localPid = localPid;
    pcbTable[slot].startSeconds = clock[0];
    pcbTable[slot].startNano = clock[1];
    pcbTable[slot].blocked = 0;

    //fork creates a child process
    //after fork:
    // pid == -1 means fork failed
    // pid == 0 means this code is running in the child
    // pid > 0 means this code is running in the parent and pid is the child's pid
    pid_t pid = fork();

    //check if fork failed
    if (pid == -1) {
        perror("OSS: Error in fork");

        //the PCB slot was marked occupied above
        //so clear it because no child has actually been created
        clearPCBEntry(slot);
        
        //remove shared memory and the message queue
        cleanupIPC();
        
        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //child process section.
    //The child immediately replaces itself with the worker executable.
    //The worker receives:
    //argv[0] = "worker"
    //argv[1] = secStr
    //argv[2] = nanoStr
    //This lets worker verify that it received a valid time limit.
    if (pid == 0) {
        execl("./worker", "worker", secStr, nanoStr, (char *) NULL);

        perror("OSS: Error in execl");
        exit(EXIT_FAILURE);
    }

    //parent process section
    //now pid is the real process id of the worker
    //store it in the PCB table so OSS can track this child
    pcbTable[slot].pid = pid;

    logBoth("\nOSS: Forked worker P%d with PID %d in slot %d at time %u:%u\n",
            pcbTable[slot].localPid,
            (int)pcbTable[slot].pid,
            slot,
            clock[0],
            clock[1]);

    // Increment the clock before sending the worker a message.
    addToClock(clock, 1000);

    // Create a message that OSS will send to the worker.
    struct Message msgToChild;

    //mtype controls which process receives the message.
    
    //The worker will call msgrcv using getpid() as the message type.
    //So OSS sets mtype equal to the worker's real pid.
    msgToChild.mtype = pid;

    //value = 1 means:
    //"Worker, take your test turn and verify shared memory."
    msgToChild.value = 1;

    //store the sender pid.
    //Here, OSS is the sender.
    msgToChild.pid = getpid();

    //store the PCB slot for this worker.
    //The worker sends this same slot back in its reply.
    msgToChild.slot = slot;

    
    //These fields are not used in the turn message.
    //They are set to safe default values anyway.
    msgToChild.address = 0;
    msgToChild.requestType = REQUEST_READ;


    logBoth("OSS: Sending test message to worker PID %d at time %u:%u\n",
            (int)pid,
            clock[0],
            clock[1]);

    //send the message to the worker.
    //
    //msgsnd arguments:
    //
    //msg_id:
    //      The message queue id.
    //
    //&msgToChild:
    //      Address of the message being sent.
    //
    //sizeof(struct Message) - sizeof(long):
    //      Size of the message data, not counting mtype.
    //
    //0:
    //      Default blocking behavior.
    //
    //msgsnd returns -1 on failure.

    //After this, the worker should generate:
    //- a random page number
    //- a random offset
    //- a logical address
    //- a read or write choice
    if (msgsnd(msg_id, &msgToChild, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: Error in msgsnd");

        //If sending fails, the worker may be waiting forever.
        //Kill it, wait for it, clear its PCB slot, and clean up IPC.
        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        clearPCBEntry(slot);
        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    // Wait for the worker to reply.
    struct Message msgFromChild;

    //wait for the worker to reply.
    //
    //The worker sends replies using mtype = 1.
    //So OSS receives messages of type 1.
    //
    //This blocks until the worker sends the message.
    if (msgrcv(msg_id, &msgFromChild, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
        perror("OSS: Error in msgrcv");

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        clearPCBEntry(slot);
        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }


    //Figure out whether the request was a read or a write.
    //This only affects the log message in step 3.
    char requestString[16];

    if (msgFromChild.requestType == REQUEST_WRITE) {
        strcpy(requestString, "write");
    } else {
        strcpy(requestString, "read");
    }

    //Extract the page number from the requested address.
    //
    //The assignment says OSS gets the page number by dividing
    //the address by 1024.
    int requestedPage = msgFromChild.address / PAGE_SIZE;

    //Extract the offset too.
    //This is not strictly required yet, but it is useful to log.
    int requestedOffset = msgFromChild.address % PAGE_SIZE;

    //Log the memory request from the worker
    logBoth("OSS: P%d requesting %s of address %d at time %u:%u\n",
            pcbTable[msgFromChild.slot].localPid,
            requestString,
            msgFromChild.address,
            clock[0],
            clock[1]);

    logBoth("OSS: Address %d is page %d with offset %d\n",
            msgFromChild.address,
            requestedPage,
            requestedOffset);

    //Step 3 says to always grant the memory request.
    //
    //So we do not check for page faults yet.
    //We do not fill the page table yet.
    //We do not fill the frame table yet.
    //
    //That starts in later steps
    addToClock(clock, 100);

    if (msgFromChild.requestType == REQUEST_WRITE) {
        logBoth("OSS: Granting write request for address %d to P%d at time %u:%u\n",
                msgFromChild.address,
                pcbTable[msgFromChild.slot].localPid,
                clock[0],
                clock[1]);
    } else {
        logBoth("OSS: Granting read request for address %d to P%d at time %u:%u\n",
                msgFromChild.address,
                pcbTable[msgFromChild.slot].localPid,
                clock[0],
                clock[1]);
    }

    //Now send a grant message back to the worker.
    //
    //The worker is waiting for this before it exits.
    struct Message grantMessage;

    //Send the grant to the worker's pid.
    grantMessage.mtype = msgFromChild.pid;

    //value = 1 means the request was granted.
    grantMessage.value = 1;

    //OSS is the sender.
    grantMessage.pid = getpid();

    //Keep the same PCB slot.
    grantMessage.slot = msgFromChild.slot;

    //Send the same address back so the worker can verify what was granted.
    grantMessage.address = msgFromChild.address;

    //Send the same request type back.
    grantMessage.requestType = msgFromChild.requestType;

    if (msgsnd(msg_id, &grantMessage, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: Error in msgsnd grant message");

        kill(pid, SIGTERM);
        waitpid(pid, NULL, 0);

        clearPCBEntry(slot);
        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    //Wait for the child process to terminate.
    if (waitpid(pid, &status, 0) == -1) {
        perror("OSS: Error in waitpid");

        //child may already be gone, but PCB slot still needs to get cleared.
        clearPCBEntry(slot);

        //clean up IPC before exiting.
        cleanupIPC();

        fclose(logFileGlobal);
        logFileGlobal = NULL;

        return EXIT_FAILURE;
    }

    logBoth("OSS: Worker PID %d has terminated.\n", (int)pid);

    //clear the PCB slot now that the worker is finished.
    //
    //This resets:
    //- occupied
    //- pid
    //- localPid
    //- start time
    //- blocked status
    //- page table entries
    clearPCBEntry(slot);

    //print tables again after worker terminates.
    //
    //At this point:
    //- PCB table should be empty again.
    //- Frame table should still be empty.
    //- No page has actually been loaded yet.
    //
    //Page/frame logic starts in later steps.
    printProcessTable(clock);
    printPageTables();
    printFrameTable();

    //clean up shared memory and the message queue.
    cleanupIPC();

    logBoth("OSS: Shared memory and message queue cleaned up.\n");
    logBoth("OSS: Step 3 complete.\n");

    //close logfile at the end.
    if (logFileGlobal != NULL) {
        fclose(logFileGlobal);
        logFileGlobal = NULL;
    }

    return EXIT_SUCCESS;
}