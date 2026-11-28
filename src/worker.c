#define _GNU_SOURCE

#include "protocol.h"

#include <poll.h>
#include <limits.h>
#include <time.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>

#define OUT_CAP PROTO_MAX_OUTPUT

typedef struct {
    int sock_fd;
    pthread_mutex_t write_mutex;
} WorkerLink;

typedef struct {
    WorkerLink *link;
    char job_id[32];
} JobLinkArg;

static long long now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int run_job(const char *cmd, char *out, ssize_t *out_len, int *truncated, int* status, int timeout_sec, int *timed_out){
	*timed_out=0;
	*out_len=0;
	*truncated=0;
	*status=0;
	int fd[2];
	if(pipe2(fd,O_CLOEXEC)==-1){
		return -1;
	}
	pid_t pid=fork();
	if(pid<0){
		close(fd[0]);
		close(fd[1]);	
		return -1;
	}
	else if (pid==0){
		setpgid(0,0);
		signal(SIGPIPE, SIG_DFL);
		int dn = open("/dev/null", O_RDONLY);
		if (dn > 0) { dup2(dn, 0); close(dn); }
		dup2(fd[1],1);
		dup2(fd[1],2);
		close(fd[0]);
		close(fd[1]);
		execl("/bin/sh","sh","-c",cmd,(char *)NULL);
		_exit(127);
	}
	else {
		setpgid(pid,pid);
		close(fd[1]);
		
		long long deadline = now_ms()+(long long)timeout_sec*1000; //deadline of how long job can be done in seconds
		long long grace_end=0; //variable making sure parent don't hangs if pipe is failed to close
		struct pollfd pf = { .fd = fd[0], .events = POLLIN, .revents = 0 }; //monitor read end of pipe to trigger when  there is data ready to read 
		char buf[4096];
		ssize_t n;
		ssize_t already_read = 0;
		
		for (;;) {
			long long limit = grace_end ? grace_end : deadline;
			long long left = limit - now_ms();	//how much a job can run more
			if (left < 0) left = 0;
			if (left > INT_MAX) left = INT_MAX; 	//set time left into size of an int
			int pr = poll(&pf, 1, (int)left); //wait for left seconds until 1 file descriptor that is pf ready to read
			if (pr < 0) {
				if (errno == EINTR) continue;
				break;
			}
			if (pr == 0) {                    // no data arrived before the limit
				if (now_ms() < limit) continue;	//if there is still time wait for more
				if (grace_end) break;         // killed, but the pipe never closed 
				*timed_out = 1;               // remember WHY, then kill 
				kill(-pid, SIGKILL);          // kill the whole process group 
				grace_end = now_ms() + 1000; //record that kill signal has been sent
				continue;
			}
			n = read(fd[0], buf, sizeof(buf));
			if (n < 0) {
				if (errno == EINTR) continue;
				break;
			}
			if (n == 0) break;                //EOF
			if (already_read < OUT_CAP) {     // keep only the bites that fir OUT_CAP 
				ssize_t room = OUT_CAP - already_read;
				ssize_t take = n < room ? n : room;
				memcpy(out + already_read, buf, (size_t)take); //write new bytes
			}
			already_read += n;                // count how many bytes are read
		}
		close(fd[0]);

		*truncated = already_read > OUT_CAP;
		if(*truncated){*out_len=(ssize_t)OUT_CAP;}
		else{*out_len=already_read;}

		struct timespec nap = { 0, 2 * 1000 * 1000 };   // 2ms 
		for (;;) {
			pid_t r = waitpid(pid, status, WNOHANG);
			if (r == pid) break; //child has finished
			if (r < 0 && errno != EINTR) return -1;
			if (!grace_end && now_ms() >= deadline) {
				*timed_out = 1; //job didnt make on time
				kill(-pid, SIGKILL); //kill the whole child process
				grace_end = now_ms() + 1000; //record that kill signal has been sent
			}
			nanosleep(&nap, NULL); //short a CPU usage because of infinite loop
		}
	return 0;
	}
}

static int connect_tcp(const char *host, const char *port)
{
    struct addrinfo hints, *res = NULL, *it;
    int fd = -1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port, &hints, &res) != 0)
        return -1;

    for (it = res; it != NULL; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0)
            break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

static int send_all(int fd, const void *data, size_t len)
{
    const char *p = (const char *)data;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

static int read_line_checked(int fd, char *buf, size_t cap)
{
    size_t used = 0;
    if (!buf || cap == 0)
        return -1;
    for (;;) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        if (c == '\n') {
            if (used > 0 && buf[used - 1] == '\r')
                --used;
            buf[used] = '\0';
            return 0;
        }
        if (used + 1 >= cap)
            return -1;
        buf[used++] = c;
    }
}

static int parse_int_strict(const char *s, int min_v, int max_v, int *out)
{
    char *end = NULL;
    long v;
    if (!s || *s == '\0' || !out)
        return -1;
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < min_v || v > max_v)
        return -1;
    *out = (int)v;
    return 0;
}

static void *job_conn_thread(void *arg)
{
    JobLinkArg *jarg = (JobLinkArg *)arg;
    WorkerLink *link = jarg->link;

    char report[256];
    int rlen = snprintf(report, sizeof(report), "%s|%s|%s|0|0|0\n",
                        PROTO_MSG_REPORT, jarg->job_id, PROTO_STATE_DONE);

    if (rlen > 0) {
        pthread_mutex_lock(&link->write_mutex);
        send_all(link->sock_fd, report, (size_t)rlen);
        pthread_mutex_unlock(&link->write_mutex);
    }

    free(jarg);
    return NULL;
}

int main(int argc, char **argv)
{
    int concurrency = 1;

    if (argc < 3 || argc > 4 || argv[1][0] == '\0' ||
        (argc == 4 && parse_int_strict(argv[3], 1, 128, &concurrency) != 0)) {
        fprintf(stderr, "Usage: %s <host> <port> [concurrency]\n", argv[0]);
        return 2;
    }

    signal(SIGPIPE, SIG_IGN);

    int fd = connect_tcp(argv[1], argv[2]);
    if (fd < 0) {
        fprintf(stderr, "worker: cannot connect to %s:%s\n", argv[1], argv[2]);
        return 3;
    }
    else {
    	printf("worker: connected to %s:%s, K=%d\n",argv[1],argv[2],concurrency);
    }

    char hello[128];
    int hlen = snprintf(hello, sizeof(hello), "%s|%d\n", PROTO_MSG_WORKER_HELLO, concurrency);
    if (hlen <= 0 || send_all(fd, hello, (size_t)hlen) != 0) {
        close(fd);
        return 1;
    }

    char line[PROTO_MAX_LINE];
    char *fields[PROTO_MAX_FIELDS];
    if (read_line_checked(fd, line, sizeof(line)) != 0 ||
        proto_split_fields(line, fields, PROTO_MAX_FIELDS) != 2 ||
        strcmp(fields[0], PROTO_OK) != 0 ||
        strcmp(fields[1], PROTO_WORKER_READY) != 0) {
        close(fd);
        return 1;
    }

    WorkerLink link;
    link.sock_fd = fd;
    pthread_mutex_init(&link.write_mutex, NULL);

    for (;;) {
        if (read_line_checked(fd, line, sizeof(line)) != 0)
            break;

        int count = proto_split_fields(line, fields, PROTO_MAX_FIELDS);
        if (count == 4 && strcmp(fields[0], PROTO_MSG_JOB) == 0) {
            JobLinkArg *jarg = calloc(1, sizeof(*jarg));
            if (!jarg)
                continue;
            jarg->link = &link;
            snprintf(jarg->job_id, sizeof(jarg->job_id), "%s", fields[1]);

            pthread_t tid;
            if (pthread_create(&tid, NULL, job_conn_thread, jarg) != 0)
                free(jarg);
            else
                pthread_detach(tid);
        } else if (count == 2 && strcmp(fields[0], PROTO_MSG_CANCEL) == 0) {
            char crep[256];
            int clen = snprintf(crep, sizeof(crep), "%s|%s|%s|0|0\n",
                                PROTO_MSG_REPORT, fields[1], PROTO_STATE_CANCELLED);
            if (clen > 0) {
                pthread_mutex_lock(&link.write_mutex);
                send_all(link.sock_fd, crep, (size_t)clen);
                pthread_mutex_unlock(&link.write_mutex);
            }
        }
    }
	
    close(fd);
    pthread_mutex_destroy(&link.write_mutex);
    return 0;
}
