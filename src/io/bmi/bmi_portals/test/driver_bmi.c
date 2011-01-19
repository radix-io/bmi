#include "portals_conn.h"
#include "portals_comm.h"
#include "portals_helpers.h"
#include "portals.h"
#include <stdio.h>
#include <pthread.h>

#define _GNU_SOURCE
#include <sched.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef BMIP_USE_MPI
#include <mpi.h>
#endif

#define NUM_RES 30
#define LIMIT 10000000

#define BMIP_NUM_BUFFERS 1

static char clone_stack[256*1024];
static char * clone_stack_top = &clone_stack[256*1024-1];
static double res_bw[NUM_RES];
static double sum_res_bw[NUM_RES];
static size_t res_len[NUM_RES];
static int rescount = 0;

typedef struct driver_arg
{
	int server;
	int rank;
	int size;
	int length;
	char * laddrstr;
	char * raddrstr;
} driver_arg_t;

int driver(void * arg);

driver_arg_t warg;
pthread_barrier_t bmi_comm_bar;
pthread_barrierattr_t bmi_comm_bar_attr;
pthread_barrier_t bmi_setup_bar;
pthread_barrierattr_t bmi_setup_bar_attr;
float bw = 0.0;

ptl_process_id_t gtarget;
pthread_t * threads = NULL;
int * tags = NULL;
size_t glength = 0;
int nthreads = 0;
struct bmi_method_addr * addr = NULL;
struct bmi_method_addr * raddr = NULL;

/* pthreads */
void * driver_client_thread(void * args);
void * driver_server_thread(void * args);

int main(int argc, char * args[])
{

	int rank = 0;
	int cstatus = 0;
	int cpid = 0;
	int ret = 0;
	int length = 0;
	int size = 0;
#ifdef BMIP_USE_MPI

	MPI_Init(&argc, &args);

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
	if(argc < 2)
	{
		fprintf(stderr, "incorrect args\n");
		return -1;
	}

	pthread_barrierattr_setpshared(&bmi_comm_bar_attr, PTHREAD_PROCESS_SHARED);
	pthread_barrier_init(&bmi_comm_bar, &bmi_comm_bar_attr, 2);
	pthread_barrierattr_setpshared(&bmi_setup_bar_attr, PTHREAD_PROCESS_SHARED);
	pthread_barrier_init(&bmi_setup_bar, &bmi_setup_bar_attr, 2);

	/* get args */
	warg.length = atoi(args[1]);
	glength = atoi(args[1]);
	nthreads = atoi(args[3]); 
	warg.laddrstr = args[4];
	warg.raddrstr = args[5];
	if(strcmp(args[2], "server") == 0)
	{
		warg.server = 1;
	}
	else
	{
		warg.server = 0;
		warg.size = size;
	}
	warg.rank = rank;

	cpid = clone(driver, clone_stack_top, CLONE_THREAD|CLONE_FILES|CLONE_SIGHAND|CLONE_VM, &warg);
	pthread_barrier_wait(&bmi_comm_bar);

	if(!warg.server)
	{
#ifdef BMIP_USE_MPI
		MPI_Reduce(res_bw, sum_res_bw, rescount, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

		if(rank == 0)
		{
			int i = 0;
			for(i = 0 ; i < rescount ; i++)
			{
				fprintf(stderr, "len = %u, avg bw = %f, aggr bw = %f\n", res_len[i], sum_res_bw[i] / size, sum_res_bw[i]);
			}
		}
	
		MPI_Barrier(MPI_COMM_WORLD);
#else
		int i = 0;
		for(i = 0 ; i < rescount ; i++)
		{
			fprintf(stderr, "len = %u, bw = %f\n", res_len[i], res_bw[i]);
		}
#endif
		pthread_barrier_wait(&bmi_comm_bar);
	}

#ifdef BMIP_USE_MPI
	MPI_Finalize();
#endif

	pthread_barrier_wait(&bmi_comm_bar);
	pthread_barrier_destroy(&bmi_comm_bar);
	return 0;
}


void * bmip_server_thread(void * args)
{
	driver_arg_t * a = (driver_arg_t *)args;

	/* init the bmi interface */
	addr = bmi_portals_ops.method_addr_lookup(a->laddrstr);
	raddr = bmi_portals_ops.method_addr_lookup(a->raddrstr);
	bmi_portals_ops.initialize(addr, portals_method_id, BMI_INIT_SERVER);

	fprintf(stderr, "nid = %i pid = %i\n", bmip_get_ptl_nid(), bmip_get_ptl_pid());

	/* wait for the other proc */
	pthread_barrier_wait(&bmi_setup_bar);

	bmip_server_monitor(NULL);

	/* shutdown */
	bmi_portals_ops.finalize();
}

int driver(void * arg)
{
	driver_arg_t * a = (driver_arg_t *)arg;

	glength = a->length;

	if(a->server)
	{
		int ret = 0;
		pthread_t server_monitor;
		int num = BMIP_NUM_BUFFERS;

		/* create the server event monitor thread */
		pthread_create(&server_monitor, NULL, bmip_server_thread, a);
	
		/* wait for the other proc */
		pthread_barrier_wait(&bmi_setup_bar);
		{
			int i = 0;

                        /* setup */
                        tags = (int *)malloc(sizeof(int) * nthreads);
                        threads = (pthread_t *)malloc(sizeof(pthread_t) * nthreads);
                        for(i = 0 ; i < nthreads ; i++)
                        {
                                tags[i] = LIMIT * 2 / nthreads * i;
                        }

                        /* create and start the threads */
                        for(i = 0 ; i < nthreads ; i++)
                        {
                                int * tid = (int *)malloc(sizeof(int));

                                *tid = i;
                                pthread_create(&threads[i], NULL, driver_server_thread, tid);
                        }

                        /* wait for threads to finish */
                        for(i = 0 ; i < nthreads ; i++)
                        {
                                pthread_join(threads[i], NULL);
                        }

                        /* cleanup */
                        free(threads);
                        free(tags);
		}
		bmip_monitor_shutdown();
		pthread_join(server_monitor, NULL);
	}
	else
	{
		int i = 0;

		/* init the bmi interface */
		addr = bmi_portals_ops.method_addr_lookup(a->laddrstr);
		raddr = bmi_portals_ops.method_addr_lookup(a->raddrstr);
		bmi_portals_ops.initialize(addr, portals_method_id, 0);

		/* setup */
		fprintf(stderr, "%s:%i nid = %i pid = %i\n", __func__, __LINE__, bmip_get_ptl_nid(), bmip_get_ptl_pid());
		{
			struct timespec s;
			struct timespec e;
	
			/* setup */	
			tags = (int *)malloc(sizeof(int) * nthreads);
			threads = (pthread_t *)malloc(sizeof(pthread_t) * nthreads);
			for(i = 0 ; i < nthreads ; i++)
			{
				tags[i] = LIMIT * 2 / nthreads * i;
			}

			bmip_get_time(&s);
			/* create and start the threads */
			for(i = 0 ; i < nthreads ; i++)
			{
				int * tid = (int *)malloc(sizeof(int));

				*tid = i;
				pthread_create(&threads[i], NULL, driver_client_thread, tid);
			}

			/* wait for threads to finish */
			for(i = 0 ; i < nthreads ; i++)
			{
				pthread_join(threads[i], NULL);
			}
			bmip_get_time(&e);

			/* cleanup */
			free(threads);
			free(tags);

			res_bw[rescount] = ((BMIP_NUM_BUFFERS * glength * 2.0 * LIMIT * 1.0)/ ( bmip_elapsed_time(&s, &e) ) ) / (1024.0  * 1024.0);
			res_len[rescount] = glength * BMIP_NUM_BUFFERS;
			rescount++;
		}
		bmi_portals_ops.finalize();

		pthread_barrier_wait(&bmi_comm_bar);
	}
	pthread_barrier_wait(&bmi_comm_bar);
	pthread_barrier_wait(&bmi_comm_bar);
	_exit(0);

	return 0;
}

void * driver_client_thread(void * args)
{
	int num = LIMIT / nthreads;
	void ** buffer = NULL;
	size_t * lengths = NULL;
	int tid = *((int *)args);
	int tag_base = tags[tid];
	int i = 0;
	
	fprintf(stderr, "%s:%i tid = %i\n", __func__, __LINE__, tid);
	/* setup */
	buffer = (void **)malloc(sizeof(void *) * BMIP_NUM_BUFFERS);
	lengths = (size_t *)malloc(sizeof(size_t) * BMIP_NUM_BUFFERS);
	for(i = 0 ; i < BMIP_NUM_BUFFERS ; i++)
	{
		buffer[i] = malloc(glength);
		lengths[i] = glength;
	}

	/* run */
	for(i = 0 ; i < num * 2 ; i += 2)
	{
		bmi_context_id cid;
		bmi_op_id_t op_id;
		bmi_size_t asize = 0;
		bmi_size_t tasize = 0;
		bmi_error_code_t ecode = 0;
		int outcount = 0;
		void * user_ptr = NULL;

		bmi_portals_ops.post_send_list(&op_id, raddr, (const void * const *)buffer, lengths, BMIP_NUM_BUFFERS, BMIP_NUM_BUFFERS * glength, 0, tag_base + i, user_ptr, cid, NULL);
		do
		{
			bmi_portals_ops.testcontext(1, &op_id, &outcount, &ecode, &asize, &user_ptr, 10, cid);
		}while(outcount == 0);

		outcount = 0;
		bmi_portals_ops.post_recv_list(&op_id, raddr, (void **)buffer, lengths, BMIP_NUM_BUFFERS, BMIP_NUM_BUFFERS * glength, &tasize, 0, tag_base + i + 1, user_ptr, cid, NULL);
		do
		{
			bmi_portals_ops.testcontext(1, &op_id, &outcount, &ecode, &asize, &user_ptr, 10, cid);
		}while(outcount == 0);
	}

	/* cleanup */
	for(i = 0 ; i < BMIP_NUM_BUFFERS ; i++)
	{
		free(buffer[i]);
	}
	free(buffer);
	free(lengths);
	free(args);

	/* exit */
	pthread_exit(NULL);
	return NULL;
}

void * driver_server_thread(void * args)
{
	int tid = *((int *)args);
	int tag_base = tags[tid];
	int i = 0;
	const int limit = LIMIT;
	const int itr = limit * 2 / nthreads;
	int num_ret = 0;

	void ** buffer = malloc(sizeof(void *) * BMIP_NUM_BUFFERS);
	size_t * lengths = malloc(sizeof(size_t) * BMIP_NUM_BUFFERS);

	fprintf(stderr, "%s:%i tid = %i\n", __func__, __LINE__, tid);
	/* setup */
	for(i = 0 ; i < BMIP_NUM_BUFFERS ; i++)
	{
		buffer[i] = bmi_portals_ops.memalloc(glength, 0);
		lengths[i] = glength;
	}

	/* run */
	for(i = 0 ; i < itr ; i+=2)
	{
		bmip_context_t * context = NULL;
		void * user_ptr = NULL;
		bmi_op_id_t oid;
		size_t size = 0;
		bmi_context_id cid;
		bmi_op_id_t op_id;
		bmi_size_t asize = 0;
		bmi_size_t tasize = 0;
		bmi_error_code_t ecode = 0;
		int outcount = 0;

		/* post the recv */
		bmi_portals_ops.post_recv_list(&op_id, raddr, (void **)buffer, lengths, BMIP_NUM_BUFFERS, BMIP_NUM_BUFFERS * glength, &tasize, 0, tag_base + i, user_ptr, cid, NULL);
		/* wait for the recv to complete */
		do
		{
			bmi_portals_ops.testcontext(1, &op_id, &outcount, &ecode, &asize, &user_ptr, 10, cid);
		}while(outcount == 0);

		outcount = 0;
		bmi_portals_ops.post_send_list(&op_id, raddr, (const void * const *)buffer, lengths, BMIP_NUM_BUFFERS, BMIP_NUM_BUFFERS * glength, 0, tag_base + i + 1, user_ptr, cid, NULL);
		do
		{
			bmi_portals_ops.testcontext(1, &op_id, &outcount, &ecode, &asize, &user_ptr, 10, cid);
		}while(outcount == 0);

		if(i % 10000 == 0)
			fprintf(stderr, "%s:%i i = %i\n", __func__, __LINE__, i);
	}

	/* cleanup */
	for(i = 0 ; i < BMIP_NUM_BUFFERS ; i++)
	{
		bmi_portals_ops.memfree(buffer[i], lengths[i], 0);
	}
	free(buffer);
	free(lengths);
	free(args);

	/* exit */
	pthread_exit(NULL);
	return NULL;
}
