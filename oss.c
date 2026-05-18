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

int main() {
    printf("Hello, I'm a step 1 placeholder! :)\n");
    return 0;
}