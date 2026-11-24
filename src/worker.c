#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

int main(void) {
	int fd[2];
	const char *cmd="ls";
	pipe(fd);
	pid_t pid=fork();
	if(pid==0){
		setpgid(0,0); //create an independent from parent process group
		dup2(fd[1],1);
		dup2(fd[1],2);
		close(fd[0]);
		close(fd[1]);
		execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
		
		perror("execl failed");
		_exit(127);
		
	}
	else if (pid>0){
		setpgid(pid,pid);
		char buf[256];
		ssize_t n;
		close(fd[1]);
		while ((n=read(fd[0],buf,sizeof(buf)-1))>0){
			buf[n]='\0';
			printf("%s",buf);
		}
		close(fd[0]);
		waitpid(pid,NULL,0);
	}
    return 0;
}
