#include <stdio.h> 
#include <stdlib.h> 
#include<time.h> 

#define RECS 200000


void gen_rand_rel(int recs){
	int rec_len = sizeof(int)* 2;
	for(int i=0 ; i<recs ; i++){
		int a = rand() % (recs/2);
		int b = rand() % (recs/2);
		printf("%d,%d\n",a,b);
	}
}

int main(){
	srand(time(0));
	gen_rand_rel(RECS);
}
