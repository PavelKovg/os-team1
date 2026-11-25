#define _POSIX_C_SOURCE 200809L

#include "journal.h"
#include "protocol.h"
#include "sheduler_policy3.h"

#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

typedef enum {
    JOB_QUEUED = 0,
    JOB_RUNNING,
    JOB_DONE,
    JOB_KILLED,
    JOB_CANCELLED
} JobState;

typedef struct JobRecord {
    char id[32];
    int timeout;
    int priority;
    char command[4096];
    JobState state;
    int exit_code;
    int signal_num;
    int timed_out;
    char *output;
    size_t output_len;
    int truncated;
    int worker_fd;
    pthread_mutex_t *worker_write_mutex;
    struct JobRecord *next;
} JobRecord;

typedef struct WorkerConn {
    int fd;
    int capacity;
    int active_jobs;
    pthread_mutex_t write_mutex;
    struct WorkerConn *next;
} WorkerConn;

typedef struct {
    Scheduler sched;
    int journal_fd;
    pthread_mutex_t journal_mutex;
    pthread_mutex_t state_mutex;
    pthread_cond_t dispatch_cond;
    JobRecord *records;
    WorkerConn *workers;
    int team_num;
    int next_job_num;
} ServerState;

typedef struct {
    ServerState *srv;
    int conn_fd;
} ConnArg;

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

static int read_exact(int fd, void *data, size_t len)
{
    char *p = (char *)data;
    while (len > 0) {
        ssize_t n = recv(fd, p, len, 0);
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

static int parse_size_strict(const char *s, size_t max_v, size_t *out)
{
    char *end = NULL;
    unsigned long long v;
    if (!s || *s == '\0' || *s == '-' || !out)
        return -1;
    errno = 0;
    v = strtoull(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v > (unsigned long long)max_v)
        return -1;
    *out = (size_t)v;
    return 0;
}

static void srv_log(ServerState *srv, const char *line)
{
    if (!srv || srv->journal_fd < 0 || !line)
        return;
    pthread_mutex_lock(&srv->journal_mutex);
    journal_append(srv->journal_fd, line);
    pthread_mutex_unlock(&srv->journal_mutex);
}

static JobRecord *find_record_locked(ServerState *srv, const char *job_id)
{
    for (JobRecord *r = srv->records; r != NULL; r = r->next) {
        if (strcmp(r->id, job_id) == 0)
            return r;
    }
    return NULL;
}

static WorkerConn *pick_worker_locked(ServerState *srv)
{
    WorkerConn *best = NULL;
    int best_free = 0;

    for (WorkerConn *w = srv->workers; w != NULL; w = w->next) {
        int free_slots = w->capacity - w->active_jobs;
        if (free_slots > best_free) {
            best_free = free_slots;
            best = w;
        }
    }
    return best;
}

static void remove_worker_locked(ServerState *srv, WorkerConn *w)
{
    WorkerConn **pp = &srv->workers;
    while (*pp != NULL) {
        if (*pp == w) {
            *pp = w->next;
            break;
        }
        pp = &(*pp)->next;
    }
    close(w->fd);
    pthread_mutex_destroy(&w->write_mutex);
    free(w);
}

static void *dispatcher_thread(void *arg)
{
    ServerState *srv = (ServerState *)arg;

    for (;;) {
        pthread_mutex_lock(&srv->state_mutex);

        while (srv->sched.count == 0 || pick_worker_locked(srv) == NULL)
            pthread_cond_wait(&srv->dispatch_cond, &srv->state_mutex);

        WorkerConn *worker = pick_worker_locked(srv);
        Job *sched_job = scheduler_next(&srv->sched);
        if (!worker || !sched_job) {
            free(sched_job);
            pthread_mutex_unlock(&srv->state_mutex);
            continue;
        }

        JobRecord *rec = find_record_locked(srv, sched_job->id);
        free(sched_job);

        if (!rec || rec->state != JOB_QUEUED) {
            pthread_mutex_unlock(&srv->state_mutex);
            continue;
        }

        char encoded_cmd[PROTO_MAX_LINE];
        char job_msg[PROTO_MAX_LINE];
        int n = -1;
        if (proto_encode_field(rec->command, encoded_cmd, sizeof(encoded_cmd)) == 0) {
            n = snprintf(job_msg, sizeof(job_msg), "%s|%s|%d|%s\n",
                         PROTO_MSG_JOB, rec->id, rec->timeout, encoded_cmd);
        }

        if (n <= 0 || (size_t)n >= sizeof(job_msg)) {
            rec->state = JOB_KILLED;
            rec->exit_code = 1;
            pthread_mutex_unlock(&srv->state_mutex);
            continue;
        }

        rec->state = JOB_RUNNING;
        rec->worker_fd = worker->fd;
        rec->worker_write_mutex = &worker->write_mutex;
        worker->active_jobs++;
        pthread_mutex_unlock(&srv->state_mutex);

        pthread_mutex_lock(&worker->write_mutex);
        int sent = send_all(worker->fd, job_msg, (size_t)n);
        pthread_mutex_unlock(&worker->write_mutex);

        if (sent != 0) {
            pthread_mutex_lock(&srv->state_mutex);
            if (rec->state == JOB_RUNNING) {
                rec->state = JOB_KILLED;
                rec->exit_code = 1;
            }
            if (worker->active_jobs > 0)
                worker->active_jobs--;
            pthread_cond_broadcast(&srv->dispatch_cond);
            pthread_mutex_unlock(&srv->state_mutex);
        }
    }
    return NULL;
}

static void *worker_thread(void *arg)
{
    ConnArg *carg = (ConnArg *)arg;
    ServerState *srv = carg->srv;
    int fd = carg->conn_fd;
    free(carg);

    char line[PROTO_MAX_LINE];
    char *fields[PROTO_MAX_FIELDS];
    int cap = 0;

    if (read_line_checked(fd, line, sizeof(line)) != 0 ||
        proto_split_fields(line, fields, PROTO_MAX_FIELDS) != 2 ||
        strcmp(fields[0], PROTO_MSG_WORKER_HELLO) != 0 ||
        parse_int_strict(fields[1], 1, 256, &cap) != 0) {
        send_all(fd, "ERR|BAD_REQUEST\n", 16);
        close(fd);
        return NULL;
    }

    WorkerConn *w = calloc(1, sizeof(*w));
    if (!w) {
        close(fd);
        return NULL;
    }
    w->fd = fd;
    w->capacity = cap;
    pthread_mutex_init(&w->write_mutex, NULL);

    pthread_mutex_lock(&srv->state_mutex);
    w->next = srv->workers;
    srv->workers = w;
    pthread_cond_broadcast(&srv->dispatch_cond);
    pthread_mutex_unlock(&srv->state_mutex);

    pthread_mutex_lock(&w->write_mutex);
    int ok = send_all(w->fd, "OK|READY\n", 9);
    pthread_mutex_unlock(&w->write_mutex);
    if (ok != 0)
        goto cleanup;

    for (;;) {
        if (read_line_checked(w->fd, line, sizeof(line)) != 0)
            break;

        int count = proto_split_fields(line, fields, PROTO_MAX_FIELDS);
        if (count < 5 || strcmp(fields[0], PROTO_MSG_REPORT) != 0)
            break;

        const char *job_id = fields[1];
        const char *status = fields[2];

        JobState new_state = JOB_DONE;
        int exit_code = 0, sig_num = 0, timed_out = 0, parsed = 0;
        size_t byte_count = 0, trunc_val = 0;

        if (strcmp(status, PROTO_STATE_DONE) == 0 && count == 6 &&
            parse_int_strict(fields[3], 0, 255, &exit_code) == 0 &&
            parse_size_strict(fields[4], PROTO_MAX_OUTPUT, &byte_count) == 0 &&
            parse_size_strict(fields[5], 1, &trunc_val) == 0) {
            new_state = JOB_DONE;
            parsed = 1;
        } else if (strcmp(status, PROTO_STATE_KILLED) == 0 &&
                   strcmp(fields[3], "TIMEOUT") == 0 && count == 6 &&
                   parse_size_strict(fields[4], PROTO_MAX_OUTPUT, &byte_count) == 0 &&
                   parse_size_strict(fields[5], 1, &trunc_val) == 0) {
            new_state = JOB_KILLED;
            timed_out = 1;
            exit_code = 124;
            parsed = 1;
        } else if (strcmp(status, PROTO_STATE_KILLED) == 0 &&
                   strcmp(fields[3], "SIGNAL") == 0 && count == 7 &&
                   parse_int_strict(fields[4], 1, 255, &sig_num) == 0 &&
                   parse_size_strict(fields[5], PROTO_MAX_OUTPUT, &byte_count) == 0 &&
                   parse_size_strict(fields[6], 1, &trunc_val) == 0) {
            new_state = JOB_KILLED;
            exit_code = 128 + sig_num;
            parsed = 1;
        } else if (strcmp(status, PROTO_STATE_CANCELLED) == 0 && count == 5 &&
                   parse_size_strict(fields[3], PROTO_MAX_OUTPUT, &byte_count) == 0 &&
                   parse_size_strict(fields[4], 1, &trunc_val) == 0) {
            new_state = JOB_CANCELLED;
            parsed = 1;
        }

        if (!parsed)
            break;

        char *out_buf = NULL;
        if (byte_count > 0) {
            out_buf = malloc(byte_count);
            if (!out_buf || read_exact(w->fd, out_buf, byte_count) != 0) {
                free(out_buf);
                break;
            }
        }

        char jline[JOURNAL_LINE];
        jline[0] = '\0';

        pthread_mutex_lock(&srv->state_mutex);
        JobRecord *rec = find_record_locked(srv, job_id);
        if (rec) {
            free(rec->output);
            rec->output = out_buf;
            rec->output_len = byte_count;
            rec->truncated = (int)trunc_val;
            rec->state = new_state;
            rec->exit_code = exit_code;
            rec->signal_num = sig_num;
            rec->timed_out = timed_out;
            rec->worker_fd = -1;
            rec->worker_write_mutex = NULL;

            if (new_state == JOB_DONE)
                snprintf(jline, sizeof(jline), "DONE %s %d", rec->id, rec->exit_code);
            else if (new_state == JOB_KILLED && timed_out)
                snprintf(jline, sizeof(jline), "KILLED_TIMEOUT %s", rec->id);
            else if (new_state == JOB_KILLED && sig_num > 0)
                snprintf(jline, sizeof(jline), "KILLED_SIGNAL %s %d", rec->id, rec->signal_num);
            else if (new_state == JOB_CANCELLED)
                snprintf(jline, sizeof(jline), "CANCEL %s", rec->id);
        } else {
            free(out_buf);
        }

        if (w->active_jobs > 0)
            w->active_jobs--;
        pthread_cond_broadcast(&srv->dispatch_cond);
        pthread_mutex_unlock(&srv->state_mutex);

        if (jline[0] != '\0')
            srv_log(srv, jline);
    }

cleanup:
    pthread_mutex_lock(&srv->state_mutex);
    for (JobRecord *r = srv->records; r != NULL; r = r->next) {
        if (r->state == JOB_RUNNING && r->worker_fd == w->fd) {
            r->state = JOB_KILLED;
            r->exit_code = 1;
            r->worker_fd = -1;
            r->worker_write_mutex = NULL;
        }
    }
    remove_worker_locked(srv, w);
    pthread_cond_broadcast(&srv->dispatch_cond);
    pthread_mutex_unlock(&srv->state_mutex);
    return NULL;
}

static void handle_submit(ServerState *srv, int fd, char **fields, int count)
{
    int timeout = 0, priority = 0;
    char decoded_cmd[4096];

    if (count != 4) {
        send_all(fd, "ERR|" PROTO_ERR_BAD_REQUEST "\n", strlen("ERR|" PROTO_ERR_BAD_REQUEST "\n"));
        return;
    }
    if (parse_int_strict(fields[1], 1, INT_MAX, &timeout) != 0) {
        send_all(fd, "ERR|" PROTO_ERR_BAD_TIMEOUT "\n", strlen("ERR|" PROTO_ERR_BAD_TIMEOUT "\n"));
        return;
    }
    if (parse_int_strict(fields[2], 0, 9, &priority) != 0) {
        send_all(fd, "ERR|" PROTO_ERR_BAD_PRIORITY "\n", strlen("ERR|" PROTO_ERR_BAD_PRIORITY "\n"));
        return;
    }
    if (proto_decode_field(fields[3], decoded_cmd, sizeof(decoded_cmd)) != 0 || decoded_cmd[0] == '\0') {
        send_all(fd, "ERR|" PROTO_ERR_BAD_REQUEST "\n", strlen("ERR|" PROTO_ERR_BAD_REQUEST "\n"));
        return;
    }

    JobRecord *rec = calloc(1, sizeof(*rec));
    Job *sjob = calloc(1, sizeof(*sjob));
    if (!rec || !sjob) {
        free(rec);
        free(sjob);
        send_all(fd, "ERR|" PROTO_ERR_INTERNAL "\n", strlen("ERR|" PROTO_ERR_INTERNAL "\n"));
        return;
    }

    pthread_mutex_lock(&srv->state_mutex);
    int num = srv->next_job_num++;
    snprintf(rec->id, sizeof(rec->id), "T%d-%d", srv->team_num, num);
    rec->timeout = timeout;
    rec->priority = priority;
    snprintf(rec->command, sizeof(rec->command), "%s", decoded_cmd);
    rec->state = JOB_QUEUED;
    rec->worker_fd = -1;
    rec->next = srv->records;
    srv->records = rec;

    snprintf(sjob->id, sizeof(sjob->id), "%s", rec->id);
    sjob->timeout = rec->timeout;
    sjob->priority = rec->priority;
    snprintf(sjob->command, sizeof(sjob->command), "%s", rec->command);

    if (scheduler_add(&srv->sched, sjob) != 0) {
        pthread_mutex_unlock(&srv->state_mutex);
        free(sjob);
        send_all(fd, "ERR|" PROTO_ERR_INTERNAL "\n", strlen("ERR|" PROTO_ERR_INTERNAL "\n"));
        return;
    }

    pthread_cond_broadcast(&srv->dispatch_cond);
    pthread_mutex_unlock(&srv->state_mutex);

    char escaped_cmd[PROTO_MAX_LINE];
    if (proto_encode_field(rec->command, escaped_cmd, sizeof(escaped_cmd)) == 0) {
        char jline[JOURNAL_LINE];
        int jn = snprintf(jline, sizeof(jline), "SUBMIT %s %d %d %s",
                          rec->id, rec->timeout, rec->priority, escaped_cmd);
        if (jn > 0 && (size_t)jn < sizeof(jline))
            srv_log(srv, jline);
    }

    char resp[128];
    int rlen = snprintf(resp, sizeof(resp), "OK|%s\n", rec->id);
    if (rlen > 0)
        send_all(fd, resp, (size_t)rlen);
}

static void handle_status(ServerState *srv, int fd, const char *job_id)
{
    char resp[128];
    int rlen = 0;

    pthread_mutex_lock(&srv->state_mutex);
    JobRecord *rec = find_record_locked(srv, job_id);
    if (!rec) {
        pthread_mutex_unlock(&srv->state_mutex);
        rlen = snprintf(resp, sizeof(resp), "ERR|%s\n", PROTO_ERR_UNKNOWN);
        send_all(fd, resp, (size_t)rlen);
        return;
    }

    switch (rec->state) {
    case JOB_QUEUED:
        rlen = snprintf(resp, sizeof(resp), "OK|%s\n", PROTO_STATE_QUEUED);
        break;
    case JOB_RUNNING:
        rlen = snprintf(resp, sizeof(resp), "OK|%s\n", PROTO_STATE_RUNNING);
        break;
    case JOB_CANCELLED:
        rlen = snprintf(resp, sizeof(resp), "OK|%s\n", PROTO_STATE_CANCELLED);
        break;
    case JOB_DONE:
        rlen = snprintf(resp, sizeof(resp), "OK|%s|%d\n", PROTO_STATE_DONE, rec->exit_code);
        break;
    case JOB_KILLED:
        if (rec->timed_out)
            rlen = snprintf(resp, sizeof(resp), "OK|%s|TIMEOUT\n", PROTO_STATE_KILLED);
        else if (rec->signal_num > 0)
            rlen = snprintf(resp, sizeof(resp), "OK|%s|SIGNAL|%d\n", PROTO_STATE_KILLED, rec->signal_num);
        else
            rlen = snprintf(resp, sizeof(resp), "OK|%s|%d\n", PROTO_STATE_DONE, rec->exit_code);
        break;
    }
    pthread_mutex_unlock(&srv->state_mutex);

    if (rlen > 0)
        send_all(fd, resp, (size_t)rlen);
}

static void handle_cancel(ServerState *srv, int fd, const char *job_id)
{
    char resp[128];
    int rlen = 0;

    pthread_mutex_lock(&srv->state_mutex);
    JobRecord *rec = find_record_locked(srv, job_id);
    if (!rec) {
        pthread_mutex_unlock(&srv->state_mutex);
        rlen = snprintf(resp, sizeof(resp), "ERR|%s\n", PROTO_ERR_UNKNOWN);
        send_all(fd, resp, (size_t)rlen);
        return;
    }

    if (rec->state == JOB_QUEUED) {
        free(scheduler_cancel(&srv->sched, job_id));
        rec->state = JOB_CANCELLED;
        pthread_mutex_unlock(&srv->state_mutex);

        char jline[128];
        if (snprintf(jline, sizeof(jline), "CANCEL %s", job_id) > 0)
            srv_log(srv, jline);

        rlen = snprintf(resp, sizeof(resp), "OK|CANCELLED\n");
        send_all(fd, resp, (size_t)rlen);
        return;
    }

    if (rec->state == JOB_RUNNING) {
        int wfd = rec->worker_fd;
        pthread_mutex_t *wmut = rec->worker_write_mutex;
        rec->state = JOB_CANCELLED;
        rec->worker_fd = -1;
        rec->worker_write_mutex = NULL;
        pthread_mutex_unlock(&srv->state_mutex);

        if (wfd >= 0 && wmut) {
            char cmsg[128];
            int clen = snprintf(cmsg, sizeof(cmsg), "%s|%s\n", PROTO_MSG_CANCEL, job_id);
            if (clen > 0) {
                pthread_mutex_lock(wmut);
                send_all(wfd, cmsg, (size_t)clen);
                pthread_mutex_unlock(wmut);
            }
        }

        char jline[128];
        if (snprintf(jline, sizeof(jline), "CANCEL %s", job_id) > 0)
            srv_log(srv, jline);

        rlen = snprintf(resp, sizeof(resp), "OK|CANCELLED\n");
        send_all(fd, resp, (size_t)rlen);
        return;
    }

    pthread_mutex_unlock(&srv->state_mutex);
    rlen = snprintf(resp, sizeof(resp), "OK|ALREADY_FINISHED\n");
    send_all(fd, resp, (size_t)rlen);
}

static void handle_fetch(ServerState *srv, int fd, const char *job_id)
{
    char resp[128];
    int rlen;

    pthread_mutex_lock(&srv->state_mutex);
    JobRecord *rec = find_record_locked(srv, job_id);
    if (!rec) {
        pthread_mutex_unlock(&srv->state_mutex);
        rlen = snprintf(resp, sizeof(resp), "ERR|%s\n", PROTO_ERR_UNKNOWN);
        send_all(fd, resp, (size_t)rlen);
        return;
    }

    if (rec->state == JOB_QUEUED || rec->state == JOB_RUNNING) {
        pthread_mutex_unlock(&srv->state_mutex);
        rlen = snprintf(resp, sizeof(resp), "ERR|%s\n", PROTO_ERR_NOT_FINISHED);
        send_all(fd, resp, (size_t)rlen);
        return;
    }

    size_t out_len = rec->output_len;
    int trunc = rec->truncated ? 1 : 0;
    char *copy = NULL;
    if (out_len > 0 && rec->output) {
        copy = malloc(out_len);
        if (!copy) {
            pthread_mutex_unlock(&srv->state_mutex);
            rlen = snprintf(resp, sizeof(resp), "ERR|%s\n", PROTO_ERR_INTERNAL);
            send_all(fd, resp, (size_t)rlen);
            return;
        }
        memcpy(copy, rec->output, out_len);
    }
    pthread_mutex_unlock(&srv->state_mutex);

    rlen = snprintf(resp, sizeof(resp), "OK|%zu|%d\n", out_len, trunc);
    if (rlen > 0 && send_all(fd, resp, (size_t)rlen) == 0 && out_len > 0)
        send_all(fd, copy, out_len);

    free(copy);
}

static void *client_thread(void *arg)
{
    ConnArg *carg = (ConnArg *)arg;
    ServerState *srv = carg->srv;
    int fd = carg->conn_fd;
    free(carg);

    char line[PROTO_MAX_LINE];
    char *fields[PROTO_MAX_FIELDS];

    if (read_line_checked(fd, line, sizeof(line)) == 0) {
        int count = proto_split_fields(line, fields, PROTO_MAX_FIELDS);
        if (count >= 1) {
            if (strcmp(fields[0], PROTO_MSG_SUBMIT) == 0)
                handle_submit(srv, fd, fields, count);
            else if (strcmp(fields[0], PROTO_MSG_STATUS) == 0 && count == 2)
                handle_status(srv, fd, fields[1]);
            else if (strcmp(fields[0], PROTO_MSG_CANCEL) == 0 && count == 2)
                handle_cancel(srv, fd, fields[1]);
            else if (strcmp(fields[0], PROTO_MSG_FETCH) == 0 && count == 2)
                handle_fetch(srv, fd, fields[1]);
            else
                send_all(fd, "ERR|" PROTO_ERR_BAD_REQUEST "\n", strlen("ERR|" PROTO_ERR_BAD_REQUEST "\n"));
        }
    }

    close(fd);
    return NULL;
}

static int open_listen_socket(int port)
{
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0)
        return -1;

    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(sfd, 64) < 0) {
        close(sfd);
        return -1;
    }
    return sfd;
}

int main(int argc, char **argv)
{
    int port = 0, team_num = 1;

    if (argc < 2 || argc > 3 ||
        parse_int_strict(argv[1], 1, 65535, &port) != 0 ||
        (argc == 3 && parse_int_strict(argv[2], 1, 999999, &team_num) != 0)) {
        fprintf(stderr, "Usage: %s <port> [team-number]\n", argv[0]);
        return 2;
    }

    signal(SIGPIPE, SIG_IGN);

    ServerState srv;
    memset(&srv, 0, sizeof(srv));
    srv.team_num = team_num;
    srv.next_job_num = 1;
    srv.journal_fd = -1;

    if (scheduler_init(&srv.sched) != 0)
        return 1;

    pthread_mutex_init(&srv.state_mutex, NULL);
    pthread_mutex_init(&srv.journal_mutex, NULL);
    pthread_cond_init(&srv.dispatch_cond, NULL);

    srv.journal_fd = journal_open("jobs.log");
    if (srv.journal_fd < 0)
        return 1;

    int listen_fd = open_listen_socket(port);
    if (listen_fd < 0) {
        close(srv.journal_fd);
        return 1;
    }

    pthread_t disp_tid;
    if (pthread_create(&disp_tid, NULL, dispatcher_thread, &srv) != 0) {
        close(listen_fd);
        close(srv.journal_fd);
        return 1;
    }
    pthread_detach(disp_tid);

    for (;;) {
        int cfd = accept(listen_fd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        char peek_buf[32];
        ssize_t pn = recv(cfd, peek_buf, sizeof(peek_buf) - 1, MSG_PEEK);
        int is_worker = 0;
        if (pn > 0) {
            peek_buf[pn] = '\0';
            if (strncmp(peek_buf, PROTO_MSG_WORKER_HELLO "|",
                        strlen(PROTO_MSG_WORKER_HELLO "|")) == 0)
                is_worker = 1;
        }

        ConnArg *carg = malloc(sizeof(*carg));
        if (!carg) {
            close(cfd);
            continue;
        }
        carg->srv = &srv;
        carg->conn_fd = cfd;

        pthread_t tid;
        if (pthread_create(&tid, NULL, is_worker ? worker_thread : client_thread, carg) != 0) {
            free(carg);
            close(cfd);
            continue;
        }
        pthread_detach(tid);
    }

    close(listen_fd);
    close(srv.journal_fd);
    return 0;
}
