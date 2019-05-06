#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_LINE_LEN 100
#define MAX_TOKENS 3

typedef struct struct_rec {
	int a;
	int b;
} rec;

rec r1[200000];
rec r2[200000];
rec r[2000000];

char *trim(char *str)
{
  char *end;

  // Trim leading space
  while(isspace((unsigned char)*str)) str++;

  if(*str == 0)  // All spaces?
    return str;

  // Trim trailing space
  end = str + strlen(str) - 1;
  while(end > str && isspace((unsigned char)*end)) end--;

  // Write new null terminator character
  end[1] = '\0';

  return str;
}


int 
split(char *buf, char *delim, char **tokens) {
    char *token = strtok(buf, delim);
    int n = 0;
    while(token) {
	tokens[n] = trim(token);
	token = strtok(NULL, delim);
	n++;
    }
    return n;
}


void load(char *f, rec r[]){
	FILE *fp = fopen(f, "r");
	char *line;
	char buf[MAX_LINE_LEN];
	 char *tokens[MAX_TOKENS];
	int i=0;
	while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL){
		int n = split(line, ",", tokens);
		//if(n!=2) printf("error\n");
		int a = atoi(tokens[0]);
		int b = atoi(tokens[1]);
		r[i].a=a;
		r[i].b=b;
		i++;
	}
	//printf("i=%d\n",i);
}

int main(int argc, char **argv){
	load(argv[1],r1);
	load(argv[2],r2);
	int n = 200000;
	int k=0;
	for(int i=0;i<n;i++){
		for(int j=0;j<n;j++){
			if(r1[i].a==r2[j].b){
				r[k].a=i;
				r[k].b=j;
				k++;
				//printf("%d,%d,%d,%d\n",r1[i].a,r1[i].b,r2[j].a,r2[j].b);
			}
		}
	}
	for(int i=0;i<k;i++){
		printf("%d,%d,%d,%d\n",r1[r[i].a].a,r1[r[i].a].b,r2[r[i].b].a,r2[r[i].b].b);
	}
}
