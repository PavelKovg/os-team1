all: server worker client

server: src/server.c src/scheduler_wooorking.c src/scheduler_policy3.h src/protocol.h src/journal.h
	gcc -Wall -pthread -o server src/server.c src/scheduler_wooorking.c

worker: src/worker.c src/protocol.h
	gcc -Wall -pthread -o worker src/worker.c

client: src/client.c src/protocol.h
	gcc -Wall -o client src/client.c

clean:
	rm -f server worker client
