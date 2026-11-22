all: server worker client

server: src/server.c
	gcc -Wall -pthread -o server src/server.c

worker: src/worker.c
	gcc -Wall -pthread -o worker src/worker.c

client: src/client.c
	gcc -Wall -o client src/client.c

clean:
	rm -f server worker client
