#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <pthread.h>
#include <stddef.h>

typedef struct Job {
    char id[32];
    int timeout;
    int priority;
    char command[4096];
    struct Job *next;
} Job;

typedef struct Scheduler {
    Job *head;
    Job *tail;
    size_t count;
    pthread_mutex_t mutex;
} Scheduler;

int scheduler_init(Scheduler *scheduler);
int scheduler_add(Scheduler *scheduler, Job *job);

Job *scheduler_next(Scheduler *scheduler);
Job *scheduler_cancel(Scheduler *scheduler, const char *job_id);

int scheduler_destroy(Scheduler *scheduler);

#endif