#ifndef PROTOCOL_H
#define PROTOCOL_H

/*
 * protocol.h - shared wire-protocol definitions and small helpers.
 *
 * Protocol v1 (proposed team contract):
 *
 *   Client -> server:
 *     SUBMIT|timeout|priority|encoded-command\n
 *     STATUS|job-id\n
 *     CANCEL|job-id\n
 *     FETCH|job-id\n
 *
 *   Server -> client:
 *     OK|job-id\n
 *     OK|QUEUED\n
 *     OK|RUNNING\n
 *     OK|DONE|exit-code\n
 *     OK|KILLED|TIMEOUT\n
 *     OK|KILLED|SIGNAL|signal-number\n
 *     OK|CANCELLED\n
 *     OK|ALREADY_FINISHED\n
 *     ERR|error-code\n
 *   FETCH success is special:
 *     OK|byte-count|truncated\n
 *     followed by exactly byte-count raw bytes (not another text line).
 *     truncated is 0 or 1.
 *
 *   Worker/server connection (proposed matching contract):
 *     Worker -> server: WORKER_HELLO|K\n
 *     Server -> worker: OK|READY\n
 *     Server -> worker: JOB|job-id|timeout|encoded-command\n
 *     Server -> worker: CANCEL|job-id\n
 *     Worker -> server: REPORT|job-id|DONE|exit-code|byte-count|truncated\n
 *       followed by byte-count raw output bytes.
 *
 *     Worker -> server: REPORT|job-id|KILLED|TIMEOUT|byte-count|truncated\n
 *       or REPORT|job-id|KILLED|SIGNAL|signal-number|byte-count|truncated\n
 *       or REPORT|job-id|CANCELLED|byte-count|truncated\n
 *       each followed by byte-count raw output bytes.
 *
 * A field is escaped by proto_encode_field() before being placed between
 '|' separators. The server must decode the SUBMIT/JOB command before exec and
 * should store an escaped one-line representation in jobs.log.
 *
 * These helpers are static inline so this header can be included by client,
 * server and worker without a separate protocol.c file.
 */

#include <errno.h>
#include <stddef.h>

#define PROTO_MAX_LINE       4096
#define PROTO_MAX_OUTPUT     65536U
#define PROTO_MAX_FIELDS     8

#define PROTO_MSG_SUBMIT     "SUBMIT"
#define PROTO_MSG_STATUS     "STATUS"
#define PROTO_MSG_CANCEL     "CANCEL"
#define PROTO_MSG_FETCH      "FETCH"
#define PROTO_MSG_WORKER_HELLO "WORKER_HELLO"
#define PROTO_MSG_JOB        "JOB"
#define PROTO_MSG_REPORT     "REPORT"

#define PROTO_OK             "OK"
#define PROTO_ERR            "ERR"
#define PROTO_WORKER_READY   "READY"

#define PROTO_STATE_QUEUED    "QUEUED"
#define PROTO_STATE_RUNNING   "RUNNING"
#define PROTO_STATE_DONE      "DONE"
#define PROTO_STATE_KILLED    "KILLED"
#define PROTO_STATE_CANCELLED "CANCELLED"

#define PROTO_ERR_UNKNOWN       "UNKNOWN"
#define PROTO_ERR_NOT_FINISHED  "NOT_FINISHED"
#define PROTO_ERR_BAD_REQUEST   "BAD_REQUEST"
#define PROTO_ERR_BAD_TIMEOUT   "BAD_TIMEOUT"
#define PROTO_ERR_BAD_PRIORITY  "BAD_PRIORITY"
#define PROTO_ERR_INTERNAL      "INTERNAL"

/* Return nonzero for bytes that must be escaped inside one protocol field. */
static inline int proto_needs_escape(unsigned char c)
{
    return c == '%' || c == '|' || c == '\n' || c == '\r';
}

/* Convert one hexadecimal digit to 0..15, or -1 if it is not hexadecimal. */
static inline int proto_hex_value(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    return -1;
}

/*
 * Encode one NUL-terminated field into dst.
 * Reserved bytes become %HH (uppercase hexadecimal).
 * Returns 0 on success, -1 on invalid arguments or insufficient space.
 * dst_size includes space for the final NUL byte.
 */
static inline int proto_encode_field(const char *src, char *dst, size_t dst_size)
{
    static const char hex[] = "0123456789ABCDEF";
    size_t used = 0;

    if (src == NULL || dst == NULL || dst_size == 0) {
        errno = EINVAL;
        return -1;
    }

    while (*src != '\0') {
        unsigned char c = (unsigned char)*src++;

        if (proto_needs_escape(c)) {
            if (used > dst_size - 1 || dst_size - 1 - used < 3) {
                errno = ENOSPC;
                return -1;
            }
            dst[used++] = '%';
            dst[used++] = hex[c >> 4];
            dst[used++] = hex[c & 0x0F];
        } else {
            if (used >= dst_size - 1) {
                errno = ENOSPC;
                return -1;
            }
            dst[used++] = (char)c;
        }
    }

    dst[used] = '\0';
    return 0;
}

/*
 * Decode a field previously encoded with proto_encode_field().
 * A decoded NUL byte is rejected because the interface uses C strings.
 * Returns 0 on success, -1 on malformed escape or insufficient space.
 */
static inline int proto_decode_field(const char *src, char *dst, size_t dst_size)
{
    size_t used = 0;

    if (src == NULL || dst == NULL || dst_size == 0) {
        errno = EINVAL;
        return -1;
    }

    while (*src != '\0') {
        unsigned char c;

        if (*src == '%') {
            int hi;
            int lo;

            if (src[1] == '\0' || src[2] == '\0') {
                errno = EINVAL;
                return -1;
            }

            hi = proto_hex_value(src[1]);
            lo = proto_hex_value(src[2]);
            if (hi < 0 || lo < 0) {
                errno = EINVAL;
                return -1;
            }

            c = (unsigned char)((hi << 4) | lo);
            if (c == '\0') {
                errno = EINVAL;
                return -1;
            }
            src += 3;
        } else {
            c = (unsigned char)*src++;
        }

        if (used >= dst_size - 1) {
            errno = ENOSPC;
            return -1;
        }
        dst[used++] = (char)c;
    }

    dst[used] = '\0';
    return 0;
}

/*
 * Split a mutable line in place at each literal '|'.
 * The line must not include its terminating newline.
 * Empty fields are preserved. Returns field count, or -1 if too many fields
 * or invalid arguments are supplied.
 */
static inline int proto_split_fields(char *line, char **fields, size_t max_fields)
{
    size_t count = 1;
    char *p;

    if (line == NULL || fields == NULL || max_fields == 0) {
        errno = EINVAL;
        return -1;
    }

    fields[0] = line;
    for (p = line; *p != '\0'; ++p) {
        if (*p == '|') {
            if (count >= max_fields) {
                errno = E2BIG;
                return -1;
            }
            *p = '\0';
            fields[count++] = p + 1;
        }
    }

    return (int)count;
}

#endif /* PROTOCOL_H */
