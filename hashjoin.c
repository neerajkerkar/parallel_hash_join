#include<string.h>
#include<stdlib.h>
#include<stdio.h>
#include <sys/mman.h>
#include<stdbool.h>
#include<pthread.h>
#include <ctype.h>

#define min(a, b) ((a) < (b)) ? (a) : (b)
#define MORSEL_SIZE 10000
#define MAX_LINE_LEN 100
#define MAX_TOKENS 3
#define removeTag(ptr) (((long) ptr) & 0x0000FFFFFFFFFFFFl)
#define TAG_MASK 0xFFFF000000000000l
#define tag(hash) ((1l << (hash & 0x000000000000000Fl)) << 48)

int sockets = 4;
typedef char byte;

#define CAS __sync_bool_compare_and_swap

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


void load(char *f, byte *storage){
	int rec_len = sizeof(int)* 2;
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
		memcpy(storage, &a, sizeof(int));
		memcpy(storage + sizeof(int), &b, sizeof(int));
		storage += rec_len;
		i++;
	}
	//printf("loaded %d tuples\n",i);
}

int join_size = 0;
pthread_mutex_t join_size_lock;



typedef struct hash_table_entry {
	long hash;
	byte *data;
	struct hash_table_entry * next;
} ht_entry;

unsigned long hash(unsigned char *str, int len){
        unsigned long hash = 0;
        int c;
	int i = 0;
        while (i < len){
	    c = str[i];
            hash = c + (hash << 6) + (hash << 16) - hash;
	    i++;
	}
        return hash;
}




void insert(ht_entry *entry, ht_entry **hashTable, int hashMask){
	int slot = entry->hash & hashMask;
	//printf("hash %lx", entry->hash);
	//printf("slot %d\n",slot);
	ht_entry *new, *old;
	do {
		old = hashTable[slot];
		entry->next = removeTag(old);
		new = ((long) entry) | (((long) old) & TAG_MASK) | tag(entry->hash);
	} while ( !CAS(&hashTable[slot], old, new));
}


typedef struct query_execution_plan_node {
	struct query_execution_plan_node *parent;
	int inp_rel_id;
	int num_dependencies;
	int dependencies_done;
	void (*task_ptr)(byte *, int, byte **, byte *, void **);
} qep_node;

typedef struct relation_metadata {
	int rec_size;
	int num_records;
	int morsel_sockets;
	byte **morsel_areas;
	int *morsel_area_sizes;
} rel_metadata;

rel_metadata metadata[10];
int next_metadata_free_slot = 0;



typedef struct struct_pipeline_job {
	void **task_args;
	qep_node * qep;
	int inp_rec_size;
	int morsel_sockets;
	byte **morsel_areas;
	int *morsel_area_sizes;
	int *morsel_area_to_be_processed;
	//int *morsel_area_processed;
	int running_tasks;
	struct struct_pipeline_job * next;
} pipeline_job;

pipeline_job * dispatcher_head = NULL;
pipeline_job * dispatcher_tail = NULL;
pthread_mutex_t dispatcher_lock; 


void task_build_hash_table(byte *morsel_start, int num_morsel_records, byte **local_storage, byte *temp_work_area, void **args){
	//printf("build task\n");
	ht_entry **hashTable = args[0];
	int hashMask = (int) args[1];
	int hash_attr_offset = (int) args[2];
	int hash_attr_len = (int) args[3];
	int record_len = (int) args[4];
	
	byte * cur_record = morsel_start;
	for(int i = 0; i < num_morsel_records; i++){
		ht_entry * entry = malloc(sizeof(ht_entry));  //replace with numa alloc
		entry->hash = hash(cur_record + hash_attr_offset, hash_attr_len);
		entry->data = cur_record;
		insert(entry, hashTable, hashMask);
		/*int attr;
		memcpy(&attr, cur_record + hash_attr_offset, hash_attr_len);
		//printf("inserted %d\n", attr);*/
		cur_record += record_len;
	}
}

int incr_indices(int *indices,int *max, int n){
	int c = 1;
	for(int i=n-1;i>=0;i--){
		indices[i] += c;
		if(indices[i] >= max[i]){
			indices[i] = indices[i] % max[i];
	 		c = 1;
		}
		else {
			c = 0;
		}
	}
	return c;
}


struct probe_task_args {
	ht_entry ***hashTables;
	int* hashMasks;
	int* build_attr_offsets;
	int* join_attr_lens;
	int* build_record_lens;
	int* join_attr_offsets;
	int record_len;
	int build_relns;
} probe_args;	

void init_probe_args(int build_relns){
	probe_args.hashTables = malloc(sizeof(ht_entry **)*build_relns);
	probe_args.hashMasks = malloc(sizeof(int)*build_relns);
	probe_args.build_attr_offsets = malloc(sizeof(int)*build_relns);
	probe_args.join_attr_lens = malloc(sizeof(int)*build_relns);
	probe_args.build_record_lens = malloc(sizeof(int)*build_relns);
	probe_args.join_attr_offsets = malloc(sizeof(int) * build_relns);
	probe_args.build_relns = build_relns;
	probe_args.record_len = metadata[0].rec_size;
}

void task_probe(byte *morsel_start, int num_morsel_records, byte **local_storage, byte *temp_work_area, void **args){
	//printf("probe task\n");
	ht_entry ***hashTables = (ht_entry ***) args[0];
	int* hashMasks = (int*) args[1];
	int* build_attr_offsets = (int*) args[2];
	int* join_attr_lens = (int*) args[3];
	int* build_record_lens = (int*) args[4];
	int* join_attr_offsets = (int*) args[5];
	int record_len = (int) args[6];
	int build_relns = (int) args[7];
	byte **temp_area = (byte **) temp_work_area;
	int *matches = calloc(build_relns, sizeof(int));

	byte *output_storage = *local_storage;
	byte * cur_record = morsel_start;
	int *indices = calloc(build_relns, sizeof(int)); 
	//int *rec_ptr_offsets = malloc(sizeof(int)*build_relns);
	//printf("build_relns %d\n",build_relns);
	
	for(int i = 0; i < num_morsel_records; i++){
		int next_free_temp = 0;
		for(int j = 0 ; j < build_relns; j++){ 
			int match = 0;
			long h = hash(cur_record + join_attr_offsets[j], join_attr_lens[j]);
			int slot = h & hashMasks[j];
			ht_entry **hashTable = hashTables[j];
			//printf("tag %lx\n",tag(h));
			if(tag(h) & ((long) hashTable[slot])){
				ht_entry *entry = removeTag(hashTable[slot]);
				while(entry != NULL){
					if(h == entry->hash){
						byte *other_record = entry->data;
						
						if(memcmp(cur_record + join_attr_offsets[j], other_record + build_attr_offsets[j], join_attr_lens[j])==0){
							temp_area[next_free_temp++] = other_record;
							match++;
							
						}
					}
					entry = entry->next;
				}
			}
			matches[j] = match;
			//printf("match %d\n",match);
		}
		bool match_found = true;
		for(int r=0;r<build_relns;r++){
			if(matches[r]==0) match_found = false;
		}
		if(match_found){
			do {
				memcpy(output_storage, cur_record, record_len);
				output_storage += record_len;
				int rec_off = 0;
				for(int rel=0 ; rel < build_relns ; rel++){
					memcpy(output_storage, temp_area[rec_off + indices[rel]], build_record_lens[rel]);
					output_storage += build_record_lens[rel];
					rec_off += matches[rel];
				}

				/*pthread_mutex_lock(&join_size_lock);
				join_size++;
				pthread_mutex_unlock(&join_size_lock);*/
					
			} while(incr_indices(indices, matches, build_relns)!=1);
		}
		cur_record += record_len;
	}
	*local_storage = output_storage;
}

int dispatch(int socket, byte **local_storage, byte *temp_storage){
	//printf("dispatch\n");
	int ret_val = 0;
	bool task_found = false;
	void (*task) (byte *, int, byte**, byte *, void **);
	void **args;
	byte *morsel_start;
	int num_morsel_records;
	pipeline_job * job;

	pthread_mutex_lock(&dispatcher_lock);
	if(dispatcher_head != NULL){
		job = dispatcher_head;
		int morsel_socket_to_process = -1;
		for(int sock = 0; sock < sockets; sock++){ //work stealing
			if(job->morsel_area_to_be_processed[sock] < job->morsel_area_sizes[sock]){
				morsel_socket_to_process = sock;
			}
			if(sock == socket) break;
		}
		if(morsel_socket_to_process != -1){
			task_found = true;
			task = job->qep->task_ptr;
			args = job->task_args;
			morsel_start = job->morsel_area_to_be_processed[morsel_socket_to_process] * job->inp_rec_size + job->morsel_areas[morsel_socket_to_process];
			num_morsel_records = min(MORSEL_SIZE, job->morsel_area_sizes[morsel_socket_to_process] -  job->morsel_area_to_be_processed[morsel_socket_to_process]);
			job->morsel_area_to_be_processed[morsel_socket_to_process] += num_morsel_records;
			job->running_tasks++;
		}
	}
	else {
		//printf("no job\n");
		ret_val = -1;
	}
	pthread_mutex_unlock(&dispatcher_lock);
	if(task_found){
		task(morsel_start, num_morsel_records, local_storage, temp_storage, args);
		pthread_mutex_lock(&dispatcher_lock);
		job->running_tasks--;
		bool pipeline_done = true;
		if(job->running_tasks > 0) pipeline_done = false;
		for(int s = 0; s < sockets; s++){
			if(job->morsel_area_to_be_processed[socket] < job->morsel_area_sizes[socket]) pipeline_done = false;
		}
		if(pipeline_done){
			//printf("pipeline done\n");
			dispatcher_head = job->next;
			//printf("dis head %p\n", dispatcher_head);
			if(dispatcher_head == NULL) dispatcher_tail = NULL;
			qep_node *pqep = job->qep->parent;
			if(pqep != NULL){
				pqep->dependencies_done++;
				//printf("dep %d %d\n",pqep->dependencies_done, pqep->num_dependencies);
				if(pqep->dependencies_done == pqep->num_dependencies){
					pipeline_job * new_job = malloc(sizeof(pipeline_job));
					void ** new_args = malloc(sizeof(void *) * 8);
					new_args[0] = probe_args.hashTables;
					new_args[1] = probe_args.hashMasks;
					new_args[2] = probe_args.build_attr_offsets;
					new_args[3] = probe_args.join_attr_lens;
					new_args[4] = probe_args.build_record_lens;
					new_args[5] = probe_args.join_attr_offsets;
					new_args[6] = probe_args.record_len;
					new_args[7] = probe_args.build_relns;
	
					new_job->task_args = new_args;
					new_job->qep = pqep;
					rel_metadata probe_relation_metadata = metadata[0]; 
					new_job->inp_rec_size = probe_relation_metadata.rec_size;
					new_job->morsel_sockets = probe_relation_metadata.morsel_sockets;
					new_job->morsel_areas = probe_relation_metadata.morsel_areas;
					new_job->morsel_area_sizes = probe_relation_metadata.morsel_area_sizes;
					new_job->morsel_area_to_be_processed = malloc(sizeof(int) * new_job->morsel_sockets);
					for(int s=0;s<new_job->morsel_sockets;s++){
						new_job->morsel_area_to_be_processed[s] = 0;
					}
					new_job->running_tasks = 0;
					new_job->next = NULL;
					if(dispatcher_tail == NULL){
						dispatcher_head = new_job;
						dispatcher_tail = new_job;
					}
					else {
						dispatcher_tail->next = new_job;
					}
				//printf("added new job\n");
				}
			}
		}
		pthread_mutex_unlock(&dispatcher_lock);
			
	}
	return ret_val;
}
	
pthread_mutex_t thread_print_lock; 

void * worker_thread(void *closest_socket){
	int socket = (int) closest_socket;
	byte * local_storage = mmap(NULL, 4 * 2 * 100000 * sizeof(int) * 4, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
	byte * temp_storage = mmap(NULL, 100000 * sizeof(int), PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
	byte * local_storage_start = local_storage;	
	while(1){
		int ret = dispatch(socket, &local_storage, temp_storage);
		if(ret==-1) break;
	}
	//printf("%d bytes written to local storage\n", local_storage - local_storage_start);
	//printf("few join result samples\n");
	int l = 4 * sizeof(int);
	byte *cr = local_storage_start;
	int total_rec_size = 0;
	for(int r=0;r<next_metadata_free_slot;r++){
		total_rec_size += metadata[r].rec_size;
	}
	//printf("%d\n",total_rec_size);
	pthread_mutex_lock(&thread_print_lock);
	while(cr < local_storage) {
		int * attr;
		for(int a=0;a<total_rec_size/sizeof(int);a++){
			attr = cr + a*sizeof(int);
			if(a>0) printf(",");
			printf("%d",*attr);
		}
		printf("\n");
		cr += total_rec_size;
	}
	pthread_mutex_unlock(&thread_print_lock);
}

int get_hash_table_size(int recs){
	int s = 1;
	while(s < recs){
		s <<= 2;
	}
	s <<= 2;
	return s;
}

void load_random_relation(int recs,byte *storage){
	int rec_len = sizeof(int)* 2;
	for(int i=0 ; i<recs ; i++){
		int a = rand() % (recs/2);
		int b = rand() % (recs/2);
		memcpy(storage, &a, sizeof(int));
		memcpy(storage + sizeof(int), &b, sizeof(int));
		storage += rec_len;
	}
}

void load_and_partition_relation(char *f){
	byte *storage = NULL, *storage_start;
	int rec_len, num_records, num_attributes;
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
		if(i==0){
			num_attributes = a;
			rec_len = a * sizeof(int);
			num_records = b;
			storage = mmap(NULL, num_records * rec_len, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
			storage_start = storage;	
		}
		else {
			memcpy(storage, &a, sizeof(int));
			memcpy(storage + sizeof(int), &b, sizeof(int));
			storage += rec_len;
		}
		i++;
	}
	rel_metadata meta;
	meta.rec_size = rec_len;
	meta.num_records = num_records;
	meta.morsel_sockets = sockets;
	byte *morsel_area = storage_start;
	int morsel_area_size_in_bytes = (num_records / sockets) * rec_len;
	meta.morsel_areas = malloc(sizeof(byte *) * sockets);
	meta.morsel_area_sizes = malloc(sizeof(int) * sockets);
	for(int s=0;s<sockets;s++){
		meta.morsel_areas[s] = morsel_area;
		morsel_area += morsel_area_size_in_bytes;
		meta.morsel_area_sizes[s] = num_records / sockets;
	}
	metadata[next_metadata_free_slot++] = meta;
	//printf("%d recs loaded from file %s\n",num_records,f);
}

int main(int argc, char **argv){
	int relations = argc/2;
	for(int r=0;r<relations;r++){
		load_and_partition_relation(argv[1+r]);
	}
	//printf("relations %d\n",relations);
	/*int rel1_len = 200000;
	int rel2_len = 200000;
	byte * rel1 = malloc(rel1_len * sizeof(int) * 2);
	byte * rel2 = malloc(rel2_len * sizeof(int) * 2);
	//load_random_relation(rel1_len, rel1);
	//load_random_relation(rel2_len, rel2);
	load("r1.csv",rel1);
	load("r2.csv",rel2);*/

	qep_node * root = malloc(sizeof(qep_node));
	root->task_ptr = task_probe;
	root->parent = NULL;
	root->num_dependencies = relations - 1;	
	root->dependencies_done = 0;

	int build_relns = relations-1;
	init_probe_args(build_relns);
	
	char *tokens[MAX_TOKENS];
	for(int c=0;c < relations-1;c++){
		int t = split(argv[1 + relations + c],",",tokens); 
		int c1 = atoi(tokens[0]);
		int c2 = atoi(tokens[1]);

		rel_metadata meta = metadata[1+c];
		qep_node * child = malloc(sizeof(qep_node));
		child->task_ptr = task_build_hash_table;
		child->parent = root;
		child->num_dependencies = 0;
		child->dependencies_done = 0;
		pipeline_job * job = malloc(sizeof(pipeline_job));
		job->qep = child;
		job->inp_rec_size = meta.rec_size;
		job->morsel_sockets = sockets;
		job->next = NULL;
		job->morsel_areas = meta.morsel_areas;
		job->morsel_area_sizes = meta.morsel_area_sizes;
		job->morsel_area_to_be_processed = calloc(sockets, sizeof(int));
		for(int s=0;s<sockets;s++){
			job->morsel_area_to_be_processed[s] = 0;
		}

		int ht_size = get_hash_table_size(meta.num_records);
		ht_entry **hashTable = mmap(NULL, ht_size * sizeof(ht_entry *), PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);	
		int h_mask = ht_size - 1;

		probe_args.hashTables[c] = hashTable;
		probe_args.hashMasks[c] = h_mask;
		probe_args.build_attr_offsets[c] = c2*sizeof(int);
		probe_args.join_attr_lens[c] = sizeof(int);
		probe_args.build_record_lens[c] = meta.rec_size;
		probe_args.join_attr_offsets[c] = c1*sizeof(int);
		
		void **args = malloc(sizeof(void *) * 5);
		args[0] = hashTable;
		args[1] = h_mask;
		args[2] = c2 * sizeof(int);
		args[3] = sizeof(int);
		args[4] = meta.rec_size;
		job->task_args = args;
		//printf("adding job\n");
		if(dispatcher_tail == NULL){
			dispatcher_head = job;
			dispatcher_tail = job;
		}
		else {
			dispatcher_tail->next = job;
			dispatcher_tail = job;
		}

	}

	pthread_t threads[4];
	for(int i=0;i<4;i++){
		pthread_create(&threads[i], NULL, worker_thread, (void *) i);
	}
	//printf("threads started\n");
	for (int i = 0; i < 4; i++) {
    		if(pthread_join(threads[i], NULL)) {
      			fprintf(stderr, "Error joining thread");
			return 2;
		}
    	}
	//printf("join size %d\n",join_size);
	return 0;
}
