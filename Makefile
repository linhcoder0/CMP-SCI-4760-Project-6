CC = gcc
CFLAGS = -g -Wall -std=c99

TARGETS = oss worker

all: $(TARGETS)

oss: oss.c
	$(CC) $(CFLAGS) -o oss oss.c

user_proc: user_proc.c
	$(CC) $(CFLAGS) -o worker worker.c

clean:
	rm -f $(TARGETS) *.o