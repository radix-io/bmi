#include "portals_conn.h"
#include "portals_comm.h"
#include "portals_helpers.h"
#include <stdio.h>

#define _GNU_SOURCE
#include <sched.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef BMIP_USE_MPI
#include <mpi.h>
#endif

#define NUM_RES 30 
static char clone_stack[256*1024];
static char * clone_stack_top = &clone_stack[256*1024-1];
static double res_bw[NUM_RES];
static double sum_res_bw[NUM_RES];
static size_t res_len[NUM_RES];
static int rescount = 0;

typedef struct driver_arg
{
	int server;
	int nid;
	int pid;
	int rank;
	int size;
	int length;
} driver_arg_t;

int driver(void * arg);

driver_arg_t warg;
pthread_barrier_t bmi_comm_bar;
pthread_barrierattr_t bmi_comm_bar_attr;
float bw = 0.0;

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

	warg.length = atoi(args[1]);
	if(strcmp(args[2], "server") == 0)
	{
		warg.server = 1;
		warg.pid = atoi(args[3]);
		warg.size = atoi(args[4]);
	}
	else
	{
		warg.server = 0;
		warg.nid = atoi(args[3]);
		warg.pid = atoi(args[4]);
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

int driver(void * arg)
{
	driver_arg_t * a = (driver_arg_t *)arg;

	if(a->server)
	{
		int ret = 0;
		int length = a->length;

		/* setup the connection on pid 312 */
		bmip_init(312);
	
		/* setup the eq */
		bmip_setup_eqs();

		{
			int i = 0;
			const int limit = 1000;
			const int itr = limit * 2 * a->size;

			for(i = 0 ; i < itr ; i++)
			{
				/* wait for a message */
				for(;;)
				{
					ret = bmip_wait_ex_event();

					/* recv'r events */
                        		if(ret == PTL_EVENT_PUT_END)
                        		{
                        			break;
                        		}
					else if(ret == PTL_EVENT_GET_END)
					{
						break;
					}
				}
			}
		}

		/* cleanup the eq */
		bmip_dest_eqs();

		/* shutdown */
		bmip_finalize();
	}
	else
	{
		size_t length = a->length;
		ptl_handle_md_t md1;
		ptl_handle_md_t md2;
		char * buffer = NULL;
		ptl_process_id_t target;
		int j = 0;
		int tag = 0;

		/* setup */
		bmip_init(303);
		target.nid = a->nid;
		target.pid = a->pid;

		bmip_setup_eqs();

		buffer = (char *)malloc(sizeof(char) * 8388608);
		{
			struct timespec s;
			struct timespec e;
			int limit = 1000;
			size_t offset = a->rank * length;
			bmip_get_time(&s);
			for(j = 0 ; j < limit * 2 ; j++)
			{
				tag = j;

				/* send the data */
				if(j % 2 == 0)
				{
					bmip_ex_msg_put(target, buffer, length, offset, tag, &md1);
				}
				else
				{
					bmip_ex_msg_get(target, buffer, length, offset, tag, &md2);
				}

				/* wait for a message */
				int ret = 0;
				int put_counter = 0;
				int get_counter = 0;

				/* until the message is done... */
				for(;;)
				{
					/* check for events */
					ret = bmip_wait_unex_event();

					/* recv'r events */
					if(ret == PTL_EVENT_REPLY_END)
					{
						get_counter++;
						if(get_counter == 2)
						{
							bmip_ptl_md_unlink(md2);
							break;
						}
					}

					/* sender events */
					else if(ret == PTL_EVENT_SEND_END)
					{
						put_counter++;
						get_counter++;
						if(put_counter == 2)
						{
							bmip_ptl_md_unlink(md1);
							break;
						}
						else if(get_counter == 2)
						{
							bmip_ptl_md_unlink(md2);
							break;
						}
					}
					else if(ret == PTL_EVENT_ACK)
					{
						put_counter++;
						if(put_counter == 2)
						{
							bmip_ptl_md_unlink(md1);
							break;
						}
					}
				}
			}
			bmip_get_time(&e);
			res_bw[rescount] = ((length * 2.0 * limit * 1.0)/ ( bmip_elapsed_time(&s, &e) ) ) / (1024.0  * 1024.0);
			res_len[rescount] = length;
			rescount++;
		}

		free(buffer);
		bmip_dest_eqs();
		bmip_finalize();

		pthread_barrier_wait(&bmi_comm_bar);
	}
	pthread_barrier_wait(&bmi_comm_bar);
	pthread_barrier_wait(&bmi_comm_bar);
	_exit(0);

	return 0;
}
