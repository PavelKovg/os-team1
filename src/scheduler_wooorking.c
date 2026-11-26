
#include "sheduler_policy3.h"
#include <errno.h> // access to standart error codes
#include <string.h> 

int scheduler_init(Scheduler *scheduler)
{
    if (scheduler == NULL) {
        errno = EINVAL;
        return -1;
    }

    scheduler->head = NULL;
    scheduler->tail = NULL;
    scheduler->count = 0;

    int rc = pthread_mutex_init(&scheduler->mutex, NULL);
    if (rc != 0) {
        errno = rc;
        return -1;
    }

    return 0;
}

int scheduler_add(Scheduler *scheduler, Job *job)
{
    if (scheduler == NULL || job == NULL || job->timeout <= 0 ||
        job->id[0] == '\0') {
        errno = EINVAL;
        return -1;
    }

    int rc = pthread_mutex_lock(&scheduler->mutex);
    if (rc != 0) {
        errno = rc;
        return -1;
    }

    for (Job *p = scheduler->head; p != NULL; p = p->next) {
        if (p == job || strcmp(p->id, job->id) == 0) {
            pthread_mutex_unlock(&scheduler->mutex);
            errno = EEXIST;
            return -1;
        }
    }

    job->next = NULL;

    if (scheduler->tail != NULL)
        scheduler->tail->next = job;
    else
        scheduler->head = job;

    scheduler->tail = job;
    scheduler->count++;

    pthread_mutex_unlock(&scheduler->mutex);

    return 0;
}

Job *scheduler_next(Scheduler *scheduler)
{
    if (scheduler == NULL) {
        errno = EINVAL;
        return NULL;
    }

    int rc = pthread_mutex_lock(&scheduler->mutex);
    if (rc != 0) {
        errno = rc;
        return NULL;
    }

    if (scheduler->head == NULL) {
        pthread_mutex_unlock(&scheduler->mutex);
        return NULL;
    }

    Job *best = scheduler->head;
    Job *best_prev = NULL;
    Job *prev = NULL;

    for (Job *current = scheduler->head; current != NULL;
         current = current->next) {
        if (current->timeout < best->timeout) {
            best = current;
            best_prev = prev;
        }
        prev = current;
    }

    if (best_prev == NULL)
        scheduler->head = best->next;
    else
        best_prev->next = best->next;

    if (scheduler->tail == best)
        scheduler->tail = best_prev;

    best->next = NULL;
    scheduler->count--;

    pthread_mutex_unlock(&scheduler->mutex);

    return best;
}

Job *scheduler_cancel(Scheduler *scheduler, const char *job_id)
{
    if (scheduler == NULL || job_id == NULL || job_id[0] == '\0') {
        errno = EINVAL;
        return NULL;
    }

    int rc = pthread_mutex_lock(&scheduler->mutex);
    if (rc != 0) {
        errno = rc;
        return NULL;
    }

    Job *prev = NULL;
    Job *current = scheduler->head;

    while (current != NULL) {
        if (strcmp(current->id, job_id) == 0) {
            if (prev == NULL)
                scheduler->head = current->next;
            else
                prev->next = current->next;

            if (scheduler->tail == current)
                scheduler->tail = prev;

            current->next = NULL;
            scheduler->count--;

            pthread_mutex_unlock(&scheduler->mutex);

            return current;
        }

        prev = current;
        current = current->next;
    }

    pthread_mutex_unlock(&scheduler->mutex);

    errno = ENOENT;
    return NULL;
}

int scheduler_destroy(Scheduler *scheduler)
{
    if (scheduler == NULL) {
        errno = EINVAL;
        return -1;
    }

    int rc = pthread_mutex_lock(&scheduler->mutex);
    if (rc != 0) {
        errno = rc;
        return -1;
    }

    if (scheduler->head != NULL) {
        pthread_mutex_unlock(&scheduler->mutex);
        errno = EBUSY;
        return -1;
    }

    pthread_mutex_unlock(&scheduler->mutex);

    rc = pthread_mutex_destroy(&scheduler->mutex);
    if (rc != 0) {
        errno = rc;
        return -1;
    }

    return 0;
}
