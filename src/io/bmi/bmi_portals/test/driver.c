#include "portals_conn.h"
#include "portals_comm.h"
#include "portals_helpers.h"
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
#define LIMIT 16

#define BMIP_NUM_BUFFERS 4

static char clone_stack[256*1024];
static char * clone_stack_top = &clone_stack[256*1024-1];
static double res_bw[NUM_RES];
static double sum_res_bw[NUM_RES];
static size_t res_len[NUM_RES];
static int rescount = 0;

typedef struct driver_arg
{
	int server;
	int pid;
	int rnid;
	int rpid;
	int rank;
	int size;
	int length;
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

	warg.length = atoi(args[1]);
	nthreads = atoi(args[4]); 
	glength = atoi(args[1]);
	if(strcmp(args[2], "server") == 0)
	{
		warg.server = 1;
		warg.pid = atoi(args[3]);
		warg.rnid = atoi(args[5]);
		warg.rpid = atoi(args[6]);
	}
	else
	{
		warg.server = 0;
		warg.pid = atoi(args[3]);
		warg.rnid = atoi(args[5]);
		warg.rpid = atoi(args[6]);
		warg.size = size;
	}
	warg.rank = rank;

	cpid = clone(driver, clone_stack_top, CLONE_THREAD|CLONE_SIGHAND|CLONE_VM, &warg);
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
	/* init the ptl interface */
	bmip_init(312);
	fprintf(stderr, "nid = %i pid = %i\n", bmip_get_ptl_nid(), bmip_get_ptl_pid());

	/* setup the eq */
	bmip_setup_eqs();

	/* wait for the other proc */
	pthread_barrier_wait(&bmi_setup_bar);

	bmip_server_monitor(NULL);

	/* cleanup the eq */
	bmip_dest_eqs();

	/* shutdown */
	bmip_finalize();
}

int driver(void * arg)
{
	driver_arg_t * a = (driver_arg_t *)arg;

	glength = a->length;
	gtarget.nid = a->rnid;
	gtarget.pid = a->rpid;

	if(a->server)
	{
		int ret = 0;
		pthread_t server_monitor;
		int num = BMIP_NUM_BUFFERS;

		/* create the server event monitor thread */
		pthread_create(&server_monitor, NULL, bmip_server_thread, NULL);
	
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
		int rescount = 0;

		/* setup */
		bmip_init(303);
		fprintf(stderr, "nid = %i pid = %i\n", bmip_get_ptl_nid(), bmip_get_ptl_pid());

		bmip_setup_eqs();
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
		bmip_dest_eqs();
		bmip_finalize();

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
	for(i = 0 ; i < num * 2 ; i++)
	{
		bmip_client_send(gtarget, BMIP_NUM_BUFFERS, buffer, lengths, i + tag_base);
		bmip_client_recv(gtarget, BMIP_NUM_BUFFERS, buffer, lengths, ++i + tag_base);
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
		buffer[i] = bmip_new_malloc(glength);
		lengths[i] = glength;
	}

	/* run */
	for(i = 0 ; i < itr ; i+=2)
	{
		bmip_context_t * context = NULL;
		void * user_ptr = NULL;
		bmi_op_id_t oid;
		size_t size = 0;

		/* post the recv */
		context = bmip_server_post_recv(gtarget, tag_base + i, BMIP_NUM_BUFFERS, buffer, lengths, BMIP_USE_CVTEST, NULL, oid);

		/* wait for the recv to complete */
		do
		{
			num_ret = bmip_server_test_events(10, 1, &user_ptr, &size, &oid);
		}while(num_ret == 0);

		/* post send */
		context = bmip_server_post_send(gtarget, tag_base + i + 1, BMIP_NUM_BUFFERS, buffer, lengths, BMIP_USE_CVTEST, NULL, oid);

		/* wait for send to complete */
		do
		{
			num_ret = bmip_server_test_events(10, 1, &user_ptr, &size, &oid);
		}while(num_ret == 0);
	}

	/* cleanup */
	for(i = 0 ; i < BMIP_NUM_BUFFERS ; i++)
	{
		bmip_new_free(buffer[i]);
	}
	free(buffer);
	free(lengths);
	free(args);

	/* exit */
	pthread_exit(NULL);
	return NULL;
}
