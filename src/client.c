#define _POSIX_C_SOURCE 200809L

#include "protocol.h"

#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>


static int connect_to_server(const char *host, const char *port) {
	struct addrinfo hints;
	struct addrinfo *results = NULL;
	struct addrinfo *it;
	int fd = -1;
	int gai_result;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	gai_result = getaddrinfo(host, port, &hints, &results);
	if (gai_result != 0) {
		errno = EHOSTUNREACH;
		return -1;
	}

	for (it = results; it != NULL; it = it->ai_next) {
		fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
		if (fd == -1)
			continue;

		if (connect(fd, it->ai_addr, it->ai_addrlen) == 0)
			break;

		close(fd);
		fd = -1;
	}

	freeaddrinfo(results);
	return fd;
}


static int send_all(int fd, const void *data, size_t length) {
	const char *p = (const char *)data;

	while (length > 0) {
		ssize_t n = send(fd, p, length, MSG_NOSIGNAL);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (n == 0) {
			errno = EPIPE;
			return -1;
		}

		p += (size_t)n;
		length -= (size_t)n;
	}

	return 0;
}


static int read_line_checked(int fd, char *buffer, size_t capacity) {
	size_t used = 0;

	if (buffer == NULL || capacity == 0) {
		errno = EINVAL;
		return -1;
	}

	for (;;) {
		char c;
		ssize_t n = recv(fd, &c, 1, 0);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (n == 0) {
			errno = ECONNRESET;
			return -1;
		}
		if (c == '\n') {
			if (used > 0 && buffer[used - 1] == '\r')
			--used;
			buffer[used] = '\0';
			return 0;
		}
		if (used + 1 >= capacity) {
			errno = EMSGSIZE;
			return -1;
		}
		buffer[used++] = c;
	}
}


static int read_exact(int fd, void *data, size_t length) {
	char *p = (char *)data;

	while (length > 0) {
		ssize_t n = recv(fd, p, length, 0);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (n == 0) {
			errno = ECONNRESET;
			return -1;
		}

		p += (size_t)n;
		length -= (size_t)n;
	}

	return 0;
}


static int write_stdout_all(const void *data, size_t length) {
	const char *p = (const char *)data;

	while (length > 0) {
		ssize_t n = write(STDOUT_FILENO, p, length);

		if (n < 0) {
			if (errno == EINTR) continue;
			return -1;
		}
		if (n == 0) {
			errno = EIO;
			return -1;
		}

		p += (size_t)n;
		length -= (size_t)n;
	}

	return 0;
}


static int parse_long_value(const char *text, long minimum, long maximum, long *value_out) {
	char *end = NULL;
	long value;

	if (text == NULL || *text == '\0' || value_out == NULL)
		return -1;

	errno = 0;
	value = strtol(text, &end, 10);
	if (errno != 0 || end == text || *end != '\0' || value < minimum || value > maximum)
		return -1;

	*value_out = value;
	return 0;
}


static int parse_size_value(const char *text, size_t *value_out) {
	char *end = NULL;
	unsigned long long value;

	if (text == NULL || *text == '\0' || value_out == NULL || *text == '-')
		return -1;

	errno = 0;
	value = strtoull(text, &end, 10);
	if (errno != 0 || end == text || *end != '\0' || value > (unsigned long long)SIZE_MAX)
		return -1;

	*value_out = (size_t)value;
	return 0;
}

static int valid_job_id(const char *id) {
	const char *p;

	if (id == NULL || id[0] != 'T')
		return 0;

	p = id + 1;
	if (*p < '0' || *p > '9') return 0;
	while (*p >= '0' && *p <= '9') ++p;

	if (*p++ != '-') return 0;
	if (*p < '0' || *p > '9') return 0;
	while (*p >= '0' && *p <= '9') ++p;

	return *p == '\0';
}


static void print_usage(const char *program) {
	fprintf(stderr,
		"Usage:\n"
		"  %s <host> <port> submit <command> <timeout> [priority]\n"
		"  %s <host> <port> status <job-id>\n"
		"  %s <host> <port> cancel <job-id>\n"
		"  %s <host> <port> fetch <job-id>\n",
		program, program, program, program);
}


static int read_response_fields(int fd, char *line, size_t line_size, char **fields, size_t max_fields, int *field_count) {
	int count;

	if (read_line_checked(fd, line, line_size) == -1) return -1;

	count = proto_split_fields(line, fields, max_fields);
	if (count < 1) return -1;

	*field_count = count;
	return 0;
}


static int handle_server_error(char **fields, int count, const char *job_id) {
	if (count != 2 || strcmp(fields[0], PROTO_ERR) != 0)
		goto protocol_error;

	if (strcmp(fields[1], PROTO_ERR_UNKNOWN) == 0 && job_id != NULL) {
		printf("%s UNKNOWN\n", job_id);
		return 2;
	}

	if (strcmp(fields[1], PROTO_ERR_NOT_FINISHED) == 0 && job_id != NULL) {
		fprintf(stderr, "%s NOT FINISHED\n", job_id);
		return 1;
	}

	fprintf(stderr, "client: server error: %s\n", fields[1]);
	return 1;

	protocol_error:
	fprintf(stderr, "client: invalid server response\n");
	return 1;
}


static int do_submit(int fd, const char *request) {
	char line[PROTO_MAX_LINE];
	char *fields[PROTO_MAX_FIELDS];
	int count;

	if (send_all(fd, request, strlen(request)) == -1 || read_response_fields(fd, line, sizeof(line), fields, PROTO_MAX_FIELDS, &count) == -1) {
		fprintf(stderr, "client: communication error\n");
		return 1;
	}

	if (count >= 1 && strcmp(fields[0], PROTO_ERR) == 0) return handle_server_error(fields, count, NULL);

	if (count != 2 || strcmp(fields[0], PROTO_OK) != 0 || !valid_job_id(fields[1])) {
		fprintf(stderr, "client: invalid server response\n");
		return 1;
	}

	printf("%s\n", fields[1]);
	return 0;
}


static int do_status(int fd, const char *request, const char *job_id) {
	char line[PROTO_MAX_LINE];
	char *fields[PROTO_MAX_FIELDS];
	int count;
	long number;

	if (send_all(fd, request, strlen(request)) == -1 || read_response_fields(fd, line, sizeof(line), fields, PROTO_MAX_FIELDS, &count) == -1) {
		fprintf(stderr, "client: communication error\n");
		return 1;
	}

	if (count >= 1 && strcmp(fields[0], PROTO_ERR) == 0) return handle_server_error(fields, count, job_id);

	if (count < 2 || strcmp(fields[0], PROTO_OK) != 0) {
		fprintf(stderr, "client: invalid server response\n");
		return 1;
	}

	if ((count == 2 && (strcmp(fields[1], PROTO_STATE_QUEUED) == 0 || strcmp(fields[1], PROTO_STATE_RUNNING) == 0 || strcmp(fields[1], PROTO_STATE_CANCELLED) == 0))) {
		printf("%s %s\n", job_id, fields[1]);
		return 0;
	}

	if (count == 3 && strcmp(fields[1], PROTO_STATE_DONE) == 0 && parse_long_value(fields[2], 0, 255, &number) == 0) {
		printf("%s DONE %ld\n", job_id, number);
		return 0;
	}

	if (count == 3 && strcmp(fields[1], PROTO_STATE_KILLED) == 0 && strcmp(fields[2], "TIMEOUT") == 0) {
		printf("%s KILLED TIMEOUT\n", job_id);
		return 0;
	}

	if (count == 4 && strcmp(fields[1], PROTO_STATE_KILLED) == 0 && strcmp(fields[2], "SIGNAL") == 0 && parse_long_value(fields[3], 1, 255, &number) == 0) {
		printf("%s KILLED SIGNAL %ld\n", job_id, number);
		return 0;
	}

	fprintf(stderr, "client: invalid server response\n");
	return 1;
}


static int do_cancel(int fd, const char *request, const char *job_id) {
	char line[PROTO_MAX_LINE];
	char *fields[PROTO_MAX_FIELDS];
	int count;

	if (send_all(fd, request, strlen(request)) == -1 || read_response_fields(fd, line, sizeof(line), fields, PROTO_MAX_FIELDS, &count) == -1) {
		fprintf(stderr, "client: communication error\n");
		return 1;
	}

	if (count >= 1 && strcmp(fields[0], PROTO_ERR) == 0)
	return handle_server_error(fields, count, job_id);

	if (count == 2 && strcmp(fields[0], PROTO_OK) == 0 && strcmp(fields[1], "CANCELLED") == 0) {
		printf("%s CANCELLED\n", job_id);
		return 0;
	}

	if (count == 2 && strcmp(fields[0], PROTO_OK) == 0 && strcmp(fields[1], "ALREADY_FINISHED") == 0) {
		printf("%s ALREADY FINISHED\n", job_id);
		return 1;
	}

	fprintf(stderr, "client: invalid server response\n");
	return 1;
}


static int do_fetch(int fd, const char *request, const char *job_id) {
	char line[PROTO_MAX_LINE];
	char *fields[PROTO_MAX_FIELDS];
	int count;
	size_t remaining;
	size_t truncated;
	unsigned char buffer[4096];

	if (send_all(fd, request, strlen(request)) == -1 || read_response_fields(fd, line, sizeof(line), fields, PROTO_MAX_FIELDS, &count) == -1) {
		fprintf(stderr, "client: communication error\n");
		return 1;
	}

	if (count >= 1 && strcmp(fields[0], PROTO_ERR) == 0)
		return handle_server_error(fields, count, job_id);

	if (count != 3 || strcmp(fields[0], PROTO_OK) != 0 || parse_size_value(fields[1], &remaining) == -1 || parse_size_value(fields[2], &truncated) == -1 || remaining > PROTO_MAX_OUTPUT || truncated > 1) {
		fprintf(stderr, "client: invalid FETCH response\n");
		return 1;
	}

	while (remaining > 0) {
		size_t chunk = remaining < sizeof(buffer) ? remaining : sizeof(buffer);

		if (read_exact(fd, buffer, chunk) == -1) {
			fprintf(stderr, "client: incomplete FETCH response\n");
			return 1;
		}
		if (write_stdout_all(buffer, chunk) == -1) {
			fprintf(stderr, "client: could not write fetched output\n");
			return 1;
			}
		remaining -= chunk;
	}

	(void)truncated;
	return 0;
}


int main(int argc, char **argv) {
	const char *host;
	const char *port_text;
	const char *verb;
	const char *job_id = NULL;
	char request[PROTO_MAX_LINE];
	int request_length;
	int fd;
	int result;
	long port_number;

	if (argc < 4) {
		print_usage(argv[0]);
		return 2;
	}

	host = argv[1];
	port_text = argv[2];
	verb = argv[3];

	if (host[0] == '\0' || parse_long_value(port_text, 1, 65535, &port_number) == -1) {
		print_usage(argv[0]);
		return 2;
	}

	(void)port_number;

	if (strcmp(verb, "submit") == 0) {
		long timeout;
		long priority = 0;
		char encoded_command[PROTO_MAX_LINE];

		if ((argc != 6 && argc != 7) || parse_long_value(argv[5], 1, INT_MAX, &timeout) == -1 || (argc == 7 && parse_long_value(argv[6], 0, 9, &priority) == -1)) {
			print_usage(argv[0]);
			return 2;
		}

		if (proto_encode_field(argv[4], encoded_command, sizeof(encoded_command)) == -1) {
			fprintf(stderr, "client: command is too long or cannot be encoded\n");
			return 2;
		}

		request_length = snprintf(request, sizeof(request), PROTO_MSG_SUBMIT "|%ld|%ld|%s\n", timeout, priority, encoded_command);
		if (request_length < 0 || (size_t)request_length >= sizeof(request)) {
			fprintf(stderr, "client: command is too long\n");
			return 2;
		}
	} else if (strcmp(verb, "status") == 0 || strcmp(verb, "cancel") == 0 || strcmp(verb, "fetch") == 0) {
			const char *message_name;

		if (argc != 5 || !valid_job_id(argv[4])) {
			print_usage(argv[0]);
			return 2;
		}

		job_id = argv[4];
		if (strcmp(verb, "status") == 0)
			message_name = PROTO_MSG_STATUS;
		else if (strcmp(verb, "cancel") == 0)
			message_name = PROTO_MSG_CANCEL;
		else
			message_name = PROTO_MSG_FETCH;

		request_length = snprintf(request, sizeof(request), "%s|%s\n",
		message_name, job_id);
		if (request_length < 0 || (size_t)request_length >= sizeof(request)) {
			fprintf(stderr, "client: request is too long\n");
			return 2;
		}
	} else {
		print_usage(argv[0]);
		return 2;
	}

	fd = connect_to_server(host, port_text);
	if (fd == -1) {
		fprintf(stderr, "client: cannot connect to %s:%s\n", host, port_text);
		return 3;
	}

	if (strcmp(verb, "submit") == 0)
		result = do_submit(fd, request);
	else if (strcmp(verb, "status") == 0)
		result = do_status(fd, request, job_id);
	else if (strcmp(verb, "cancel") == 0)
		result = do_cancel(fd, request, job_id);
	else
		result = do_fetch(fd, request, job_id);

	close(fd);
	return result;
}
