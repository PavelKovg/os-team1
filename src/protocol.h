#ifndef PROTOCOL_H
#define PROTOCOL_H

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


static inline int proto_needs_escape(unsigned char c) {
	return c == '%' || c == '|' || c == '\n' || c == '\r';
}


static inline int proto_hex_value(char c) {
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	return -1;
}


static inline int proto_encode_field(const char *src, char *dst, size_t dst_size) {
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


static inline int proto_decode_field(const char *src, char *dst, size_t dst_size) {
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


static inline int proto_split_fields(char *line, char **fields, size_t max_fields) {
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
