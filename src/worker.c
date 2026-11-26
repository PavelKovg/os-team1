#define _POSIX_C_SOURCE 200809L

#include "protocol.h"

#include <errno.h>
#include <netdb.h> // Provides host/service resolution (getaddrinfo, freeaddrinfo, struct addrinfo) so the worker can connect to a <host> and <port>.
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
    int sock_fd;
    pthread_mutex_t write_mutex;
} WorkerLink;

typedef struct {
    WorkerLink *link;
    char job_id[32];
} JobLinkArg;

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
