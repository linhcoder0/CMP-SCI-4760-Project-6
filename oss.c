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
#include <signal.h>
#include <errno.h>
#include <stdarg.h>

#define NANOPERSEC 1000000000LL
#define MAX_LOG_LINES 10000

//shared memory clock size
//like the previous project, we will use an array of two unsigned ints to represent the clock,
//where clock[0] is seconds and clock[1] is nanoseconds. This allows us to easily handle the clock math and normalization.
const size_t BUFF_SZ = sizeof(unsigned int) * 2;

//global shared memory id.
//this is global so cleanupIPC can remove the shared memory segment even if the program is interrupted by a signal.
static int shm_id_global = -1;

//global pointer to the shared memory clock
//this is global so cleanupIPC can detach from it even if the program exits early.
static unsigned int *clock_global = NULL;


static FILE *logFileGlobal = NULL;
static int logLineCount = 0;
static int logLimitMessagePrinted = 0;

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

//cleanup all IPC created by OSS.
//for task 1 the only IPC resource is shared memory. 
void cleanupIPC() {
//detach from shared memory if OSS is currently attached
//shmat returns (void *) -1 on failure, so we also make sure clock_global is not that error value.

    if (clock_global != NULL && clock_global != (void *) -1) {
        if (shmdt(clock_global) == -1) {
            perror("OSS: Error in shmdt");
        }

        clock_global = NULL;
    }

// remove the shared memory segment if it was created successfully.
// IPC_RMID marks the shm segment for deletion.
// this is required so we do not leave shm behind after program ends.

    if (shm_id_global != -1) {
        if (shmctl(shm_id_global, IPC_RMID, NULL) == -1) {
            perror("OSS: Error in shmctl");
        }

        shm_id_global = -1;
    }
}

//lets OSS clean up shared memory if the user presses CTRL+C or the program receives a term signal.
//This is important because if the program is interrupted without cleaning up,
// it can leave shared memory segments behind in the system, 
//which can cause issues if you try to run the program again or if other programs use shared memory.
void signal_handler(int sig) {
    logBoth("OSS: received signal %d, shutting down...\n", sig);

    cleanupIPC();

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

    //open log file for writing and save the file pointer globally so signal handler can close it on shutdown.
    logFileGlobal = fopen(logFile, "w");

    if (logFileGlobal == NULL) {
        perror("OSS: Error opening log file");
        return EXIT_FAILURE;
    }

     //set up signal handlers after the logfile is open so the signal handler can log the shutdown message
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

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

    
     //task 1 says to use the shared memory.
     //what we are doing right now is modifying the clock in shared memory by adding time to it. 
     //This proves that oss can write to shared memory and that the clock math works correctly.
    addToClock(clock, 500);

    logBoth("OSS: Clock after adding 500 nanoseconds is %u:%u\n",
            clock[0],
            clock[1]);


     //add to clock again with a larger value, this time we add 1 second worth of nanoseconds to the clock.
     //This should cause the clock to roll over to 1 second and 500 nanoseconds
    addToClock(clock, 1000000000LL);

    logBoth("OSS: Clock after adding 1 second is %u:%u\n",
            clock[0],
            clock[1]);

    //cleanup shared memory by detaching from it and marking it for deletion.
    cleanupIPC();

    logBoth("OSS: Shared memory detached and removed.\n");

    //close the log file
    if (logFileGlobal != NULL) {
        fclose(logFileGlobal);
        logFileGlobal = NULL;
    }

    return EXIT_SUCCESS;
}
