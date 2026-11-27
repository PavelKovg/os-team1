#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>

void run_job(char *cmd){
	int truncated=0;
	int fd[2];
	pipe(fd);
	pid_t pid=fork();
	if (pid==0){
		setpgid(0,0);
		dup2(fd[1],1);
		dup2(fd[1],2);
		close(fd[0]);
		close(fd[1]);
		execl("/bin/sh","sh","-c",cmd,(char *)NULL);
		_exit(127);
	}
	else if(pid>0){
		setpgid(pid,pid);
		close(fd[1]);
		char buf[4096];
		int already_read=0;
		int status;
		int n;
		while((n=read(fd[0],buf,sizeof(buf)))>0){}
		close(fd[0]);
		waitpid(pid, &status,0);
	}
}

int main(void) {
    char *cmds[] = {
        "echo hello",
        "exit 3",
        "ls /nonexistent",
        "yes | head -c 200000",
    };

    for (int i = 0; i < 4; i++) {
        printf("Running: [%s]\n", cmds[i]);
        run_job(cmds[i]);
        printf("Finished: [%s]\n\n", cmds[i]);
    }

    return 0;
}
