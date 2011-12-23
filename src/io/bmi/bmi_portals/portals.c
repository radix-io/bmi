#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>

#include "portals_conn.h"
#include "portals_comm.h"
#include "portals_helpers.h"
#include "portals_trace.h"

#include "src/common/quicklist/quicklist.h"
#include "src/common/gen-locks/gen-locks.h"
#include "src/common/id-generator/id-generator.h"
#include "src/io/bmi/bmi.h"
#include "src/io/bmi/bmi-method-support.h"
#include "src/io/bmi/bmi-method-callback.h"

/* bmi mode */
#define CLIENT 0
#define SERVER 1

#define BMIP_CLONE

#ifdef BMIP_CLONE
static int client_clone_mode = 1;
#else
static int client_clone_mode = 0;
#endif

/* thread info */
static pthread_t bmip_monitor_thread;

/* portals id info */
static int portals_node_type;
int portals_method_id;

/* clone stack data */
#define BMIP_CLONE_STACK_SIZE 262144 
static char clone_stack[BMIP_CLONE_STACK_SIZE];
static char * clone_stack_top = &clone_stack[BMIP_CLONE_STACK_SIZE - 1];
static pid_t client_clone_pid = 0;

/* list of portals addrs */
static QLIST_HEAD(bmip_addr_list);

/* lock for accessing the addr list */
static gen_mutex_t addr_lock = GEN_MUTEX_INITIALIZER;
static gen_mutex_t server_lock = GEN_MUTEX_INITIALIZER;

/* delegate data */
static ptl_process_id_t bmip_delegate_pid = {.pid = 0, .nid = 0};
static void * bmip_delegate_buffer = NULL;
static size_t bmip_delegate_size = 0;
static int64_t bmip_delegate_tag = 0;
static int bmip_delegate_ret = 0;
static int bmip_delegate_list_count = 0;
static void ** bmip_delegate_buffer_list = NULL;
static size_t * bmip_delegate_size_list = NULL;
static int bmip_client_shutdown = 0;

typedef enum
{
	BMIP_UNKNOWN = 0,
	BMIP_POSTSEND,
	BMIP_POSTSENDLIST,
	BMIP_POSTSENDUNEX,
	BMIP_POSTRECV,
	BMIP_POSTRECVLIST,
	BMIP_FINALIZE,
} bmip_client_op_t;

bmip_client_op_t bmip_client_op_type = BMIP_UNKNOWN; 

static void BMI_portals_client_post_send(void);
static void BMI_portals_client_post_send_list(void);
static void BMI_portals_client_post_sendunexpected(void);
static void BMI_portals_client_post_recv(void);
static void BMI_portals_client_post_recv_list(void);
static void BMI_portals_client_finalize(void);

/* container for portals specific addr info */
typedef struct portals_addr
{
	struct qlist_head list;
	ptl_process_id_t pid;
	char * hostname;
	bmi_method_addr_p p_addr;
} portals_addr_t;

static int bmip_is_clone = 0;
pthread_barrier_t bmip_comm_bar;
pthread_barrierattr_t bmi_comm_bar_attr;

static void * bmiptl_safe_malloc(size_t size, char * fl, char * fn, int l)
{
        void * m = NULL;

        m = malloc(size);
        if(!m)
        {
                fprintf(stderr, "%s:%i malloc failed. size = %lu\n", __func__, __LINE__, size);
                assert(m != NULL);
        }

        return m;
}

static void bmiptl_safe_free(void * m, char * fl, char * fn, int l)
{
        if(!m)
        {
                fprintf(stderr, "%s:%i free failed. NULL buffer detected\n", __func__, __LINE__);
                assert(m != NULL);
        }
        free(m);
}

int bmip_comm_barrier_init(void)
{
	int ret = 0;
    	ret = pthread_barrierattr_setpshared(&bmi_comm_bar_attr, PTHREAD_PROCESS_SHARED);
    	if(ret == -1)
    	{
        	perror("could not set barrier attr\n");
    	}
    	pthread_barrier_init(&bmip_comm_bar, &bmi_comm_bar_attr, 2);
    	return 0;
}

int bmip_comm_barrier_destroy(void)
{
    return pthread_barrier_destroy(&bmip_comm_bar);
}

int bmip_comm_barrier(void)
{
	int ret = 0;
	ret = pthread_barrier_wait(&bmip_comm_bar);

	if(ret != PTHREAD_BARRIER_SERIAL_THREAD && ret != 0)
	{
        	perror("could not wait at barrier\n");
	}
    	return ret;
}

void bmip_client_sh(int sig)
{
	/* force a seg fault */
	{ int *_int_p = 0L; *_int_p = 1; }
}

int bmip_client_clone_init(void * args)
{
	bmip_is_clone = 1;
	bmi_method_addr_p listen_addr = (bmi_method_addr_p)args;

	/* signal handler for debug mode */
	//signal(SIGINT, bmip_client_sh);
	
	/* if we have the addr */
	if(listen_addr)
	{
		portals_addr_t * a = ((portals_addr_t *)listen_addr->method_data);
	
		/* init the bmip layer */
		bmip_init(a->pid.pid);
	}
	else
	{
		/* init the bmip layer */
		bmip_init(303);
	}

	/* setup the eq lists */
	bmip_setup_eqs(0);

	bmip_comm_barrier();

	/* wait for work */
	while(!bmip_client_shutdown)
	{
		/* wait for the bmi caller to signal */
		bmip_comm_barrier();

		/* delegate the bmi call */
		switch(bmip_client_op_type)
		{
			case BMIP_POSTSEND:
				BMI_portals_client_post_send();
				break;
			case BMIP_POSTSENDLIST:
				BMI_portals_client_post_send_list();
				break;
			case BMIP_POSTSENDUNEX:
				BMI_portals_client_post_sendunexpected();
				break;
			case BMIP_POSTRECV:
				BMI_portals_client_post_recv();
				break;
			case BMIP_POSTRECVLIST:
				BMI_portals_client_post_recv_list();
				break;
			case BMIP_FINALIZE:
				BMI_portals_client_finalize();
				bmip_client_shutdown = 1;
				break;
			default:
				break;
		};

		/* signal the caller */
		bmip_comm_barrier();
	}

	return 0;
}

static int
BMI_portals_initialize(bmi_method_addr_p listen_addr, int method_id,
			int init_flags)
{
	portals_node_type = (init_flags & BMI_INIT_SERVER) ? SERVER : CLIENT;
	portals_method_id = method_id;

	/* dynamically allocate the correct mem */
	if(portals_node_type == SERVER)
	{
		bmip_allocate_server_mem();
	}
	else if(portals_node_type == CLIENT)
	{
		bmip_allocate_client_mem();
	}
	
	if(portals_node_type == SERVER || (!client_clone_mode && portals_node_type == CLIENT) )
	{
		/* if we have the addr */
		if(listen_addr)
		{
			portals_addr_t * a = ((portals_addr_t *)listen_addr->method_data);
	
			/* init the bmip layer */
			bmip_init(a->pid.pid);
		}
		else
		{
			/* init the bmip layer */
			bmip_init(303);
		}	
	}

	/* setup the eq lists */
	if(portals_node_type == SERVER)
		bmip_setup_eqs(1);
	else
		bmip_setup_eqs(0);

	if(portals_node_type == SERVER)
	{
		pthread_create(&bmip_monitor_thread, NULL, bmip_server_monitor, NULL);
	}
	else if(portals_node_type == CLIENT)
	{
		bmip_comm_barrier_init();
#ifdef BMIP_CLONE
		client_clone_pid = clone(bmip_client_clone_init, clone_stack_top, CLONE_THREAD|CLONE_SIGHAND|CLONE_VM, listen_addr);
		bmip_comm_barrier();
#endif
	}

	return 0;
}

/* Invoked on BMI_finalize.  */
static int
BMI_portals_finalize(void)
{
	bmip_trace_dump_list();
	if(portals_node_type == SERVER)
	{
		bmip_dest_eqs();
		bmip_finalize();
		return 0;
	}
	else if(portals_node_type == CLIENT && !bmip_is_clone)
	{
		bmip_client_op_type = BMIP_FINALIZE;
		bmip_delegate_ret = 0;
		bmip_comm_barrier();
		bmip_comm_barrier();
		return bmip_delegate_ret;
	}

	return 0;
}

static void
BMI_portals_client_finalize(void)
{
	if(bmip_is_clone && portals_node_type == CLIENT)
	{
		bmip_dest_eqs();
		bmip_finalize();
		bmip_delegate_ret = 0;
	}
}

/* Invoked on BMI_set_info.  The only important case seems to be an internal
   invocation to release a no longer needed address.  */
static int
BMI_portals_set_info(int option, void* inout_parameter)
{
	switch (option)
	{
		case BMI_DROP_ADDR:
		{
			gen_mutex_lock(&addr_lock);
			bmi_method_addr_p addr = (bmi_method_addr_p)inout_parameter;
			portals_addr_t * a = (portals_addr_t *)addr->method_data;

			/* remove from the list */
			qlist_del(&a->list);
			gen_mutex_unlock(&addr_lock);

			break;
		}
		case BMI_OPTIMISTIC_BUFFER_REG:
			break;
		default:
			break;
	};
	return 0;
}

/* Invoked on BMI_get_info.  */
static int
BMI_portals_get_info(int option, void* inout_parameter)
{
	int ret = 0;

	switch (option)
	{
		case BMI_CHECK_MAXSIZE:
		{
			*(int *)inout_parameter = bmip_get_max_ex_msg_size();
			break;
		}
		case BMI_GET_UNEXP_SIZE:
		{
			*(int *)inout_parameter = bmip_get_max_unex_msg_size();
			break;
		}
		default:
		{
			ret = -1;
		}
	};
	return ret;
}

/* Invoked on BMI_memalloc.  Important on the server, not so much on the
   client.  */
static void*
BMI_portals_memalloc(bmi_size_t size, enum bmi_op_type send_recv)
{
	if(portals_node_type == SERVER)
	{
		return bmip_new_malloc(size);
	}
	else
	{
		return bmiptl_safe_malloc(size, __FILE__, __func__, __LINE__);
	}
	return NULL;
}

/* Invoked on BMI_memfree.  */
static int
BMI_portals_memfree(void* buffer, bmi_size_t size, enum bmi_op_type send_recv)
{
	int ret = 0;
	if(portals_node_type == SERVER)
	{
		bmip_new_free(buffer);
	}
	else
	{
		bmiptl_safe_free(buffer, __FILE__, __func__, __LINE__);
	}
	return ret;
}

/* Invoked on BMI_unexpected_free.  We only support in on the server.  */
static int
BMI_portals_unexpected_free(void* buffer)
{
	bmiptl_safe_free(buffer, __FILE__, __func__, __LINE__);

	return 0;
}

/* Invoked on BMI_post_send.  */
static int
BMI_portals_post_send(bmi_op_id_t* id, bmi_method_addr_p dest,
		   const void* buffer, bmi_size_t size,
		   enum bmi_buffer_type buffer_type, bmi_msg_tag_t tag,
		   void* user_ptr, bmi_context_id context_id, PVFS_hint hints)
{
	if(portals_node_type == SERVER)
	{
		method_op_p mop = bmi_alloc_method_op(0);
    		mop->addr = dest;
    		mop->method_data = NULL;
    		mop->user_ptr = user_ptr;
    		mop->context_id = context_id;
		*id = mop->op_id;

		bmip_server_post_send(((portals_addr_t *)dest->method_data)->pid, (int64_t)tag, 1, (void **)&buffer, (size_t *)&size, BMIP_USE_CVTEST, user_ptr, *id);
		return 0;
	}
	else
	{
		/* delegate data */
		bmip_delegate_pid = ((portals_addr_t *)dest->method_data)->pid;
		bmip_delegate_buffer = buffer;
		bmip_delegate_size = size;
		bmip_delegate_tag = tag;
		bmip_delegate_ret = 0;
		bmip_client_op_type = BMIP_POSTSEND;

		bmip_comm_barrier();
		bmip_comm_barrier();

		return bmip_delegate_ret;
	}
	return -1;
}

static void 
BMI_portals_client_post_send(void)
{
	bmip_client_send(bmip_delegate_pid, 1, &bmip_delegate_buffer, (size_t *)&bmip_delegate_size, (int64_t)bmip_delegate_tag);
	bmip_delegate_ret = 1;
}

/* Invoked on BMI_post_sendunexpected.  We only support it on clients.  */
static int
BMI_portals_post_sendunexpected(bmi_op_id_t* id, bmi_method_addr_p dest,
				 const void* buffer, bmi_size_t size,
				 enum bmi_buffer_type buffer_type,
				 bmi_msg_tag_t tag, void* user_ptr,
				 bmi_context_id context_id, PVFS_hint hints)
{
	int ret = 0;
	if(portals_node_type == CLIENT)
	{
		bmip_delegate_pid = ((portals_addr_t *)dest->method_data)->pid;
		bmip_delegate_buffer = buffer;
		bmip_delegate_size = size;
		bmip_delegate_tag = tag;
		bmip_delegate_ret = 0;
		bmip_client_op_type = BMIP_POSTSENDUNEX;

		bmip_comm_barrier();
		bmip_comm_barrier();

		return bmip_delegate_ret;
	}
	else
	{
		method_op_p mop = bmi_alloc_method_op(0);
    		mop->addr = dest;
    		mop->method_data = NULL;
    		mop->user_ptr = user_ptr;
    		mop->context_id = context_id;
		*id = mop->op_id;

		return bmip_server_post_unex_send(((portals_addr_t *)dest->method_data)->pid,
				1, buffer, size, tag, user_ptr, *id);
	}
	return -1;
}

static void
BMI_portals_client_post_sendunexpected(void)
{
	bmip_client_unex_send(bmip_delegate_pid, 1, bmip_delegate_buffer, bmip_delegate_size, bmip_delegate_tag);
	bmip_delegate_ret = 1;
}

/* Invoked on BMI_post_recv.  */
static int
BMI_portals_post_recv(bmi_op_id_t* id, bmi_method_addr_p src, void* buffer,
		   bmi_size_t expected_size, bmi_size_t* actual_size,
		   enum bmi_buffer_type buffer_type, bmi_msg_tag_t tag,
		   void* user_ptr, bmi_context_id context_id, PVFS_hint hints)
{
	int ret = 0;

	if(portals_node_type == SERVER)
	{
		method_op_p mop = bmi_alloc_method_op(0);
    		mop->addr = src;
    		mop->method_data = NULL;
    		mop->user_ptr = user_ptr;
    		mop->context_id = context_id;
		*id = mop->op_id;

		bmip_server_post_recv(((portals_addr_t *)src->method_data)->pid, (int64_t)tag, 1, (void **)&buffer, (size_t *)&expected_size, BMIP_USE_CVTEST, user_ptr, *id);
		*actual_size = expected_size;
		return 0;
	}
	else
	{
		bmip_delegate_pid = ((portals_addr_t *)src->method_data)->pid;
		bmip_delegate_buffer = buffer;
		bmip_delegate_size = expected_size;
		bmip_delegate_tag = tag;
		bmip_delegate_ret = 0;
		bmip_client_op_type = BMIP_POSTRECV;

		bmip_comm_barrier();
		bmip_comm_barrier();

		*actual_size = bmip_delegate_size;

		return bmip_delegate_ret;
	}
	return -1;
}

static void
BMI_portals_client_post_recv(void)
{
	bmip_client_recv(bmip_delegate_pid, 1, (void **)&bmip_delegate_buffer, (size_t *)&bmip_delegate_size, (int64_t)bmip_delegate_tag);
	bmip_delegate_ret = 1;
}

/* Invoked on BMI_post_test.  */
static int
BMI_portals_test(bmi_op_id_t id, int* outcount, bmi_error_code_t* error_code,
		  bmi_size_t* actual_size, void** user_ptr, int max_idle_time_ms,
		  bmi_context_id context_id)
{
	/* do nothing... we don't use this method */
	if(portals_node_type == SERVER)
	{
		method_op_p op = (method_op_p)id_gen_fast_lookup(id);
		*outcount = bmip_server_test_event_id(max_idle_time_ms, 1, user_ptr, (size_t *)actual_size, id);
		*error_code = 0;

		if(*outcount > 0)
		{
			bmi_dealloc_method_op(op);
		}
		return 0;
	}
	/* client only tracks operations */
	else
	{
		/* release the op and return */
		*outcount = 1;
		*error_code = 0;
		*user_ptr = NULL;
		return 1;
	}
	return -1;
}

/* Invoked on BMI_post_testsome.  */
static int
BMI_portals_testsome(int incount, bmi_op_id_t* id_array, int* outcount,
		  int* index_array, bmi_error_code_t* error_code_array,
		  bmi_size_t* actual_size_array, void** user_ptr_array,
		  int max_idle_time_ms, bmi_context_id context_id)
{
	/* do nothing... we don't use this method */
	return 0;
}

/* Invoked on BMI_testcontext.  */
static int
BMI_portals_testcontext(int incount, bmi_op_id_t* out_id_array, int* outcount,
			 bmi_error_code_t* error_code_array,
			 bmi_size_t* actual_size_array, void** user_ptr_array,
			 int max_idle_time_ms, bmi_context_id context_id)
{
	if(portals_node_type == SERVER)
	{
		int64_t * opids = (int64_t *)bmiptl_safe_malloc(sizeof(int64_t) * incount, __FILE__, __func__, __LINE__);
		size_t * sizes = (size_t *)bmiptl_safe_malloc(sizeof(size_t) * incount, __FILE__, __func__, __LINE__);

		*outcount = bmip_server_test_events(max_idle_time_ms, incount, user_ptr_array, sizes, opids);

		if(*outcount > 0)
		{
			int i = 0;

			/* iterate over the ret values */
			for(i = 0 ; i < *outcount ; i++)
			{
				error_code_array[i] = 0;
				actual_size_array[i] = (bmi_size_t)sizes[i];
				out_id_array[i] = (bmi_op_id_t)opids[i];

				/* dealloc the op */
				struct method_op * mop = (struct method_op *)id_gen_fast_lookup(out_id_array[i]);
				bmi_dealloc_method_op(mop);
			}
		}

		bmiptl_safe_free(opids, __FILE__, __func__, __LINE__);
		bmiptl_safe_free(sizes, __FILE__, __func__, __LINE__);

		return 0;
	}
	/* client only tracks operations */
	else
	{
		/* release the op and return */
		*outcount = 1;
		error_code_array[0] = 0;
		user_ptr_array[0] = NULL;
		return 0;
	}
	return -1;
}

bmi_method_addr_p bmip_lookup_addr(ptl_process_id_t pid)
{
	int found = 0;
	char id[25];
	bmi_method_addr_p addr = NULL;
	portals_addr_t * a = NULL, * n_a = NULL;

	/* portals://nid00000:000 */
	sprintf(&id[0], "portals://nid%05i:%03i\n", pid.nid, pid.pid);
	
	/* scan the list of known addrs for this addr */
	gen_mutex_lock(&addr_lock);
	qlist_for_each_entry_safe(a, n_a, &bmip_addr_list, list)
	{
			/* if we found a match */
			if(strcmp(id, a->hostname) == 0)
			{
				found = 1;
				break;
			}
	}

	/* if we did not find the address, make a new one and add it to the list */
	if(!found)
	{
		if(!(addr = bmi_alloc_method_addr(portals_method_id, sizeof(portals_addr_t))))
		{
				gen_mutex_unlock(&addr_lock);
				return NULL;
		}

		/* store the pid and nid */
		((portals_addr_t *)(addr->method_data))->pid.nid = pid.nid;
		((portals_addr_t *)(addr->method_data))->pid.pid = pid.pid;
		((portals_addr_t *)(addr->method_data))->p_addr = addr;

		/* store a copy of the hostname */
		((portals_addr_t *)(addr->method_data))->hostname = strdup(id);

		/* add it to the list */
		qlist_add_tail(&((portals_addr_t *)(addr->method_data))->list, &bmip_addr_list);

        	bmi_method_addr_reg_callback(addr);
	}
	else
	{
		addr = a->p_addr;
	}

	gen_mutex_unlock(&addr_lock);
	return addr;
}

/* Invoked on BMI_testunexpected.  We only support in on the server.  */
static int
BMI_portals_testunexpected(int incount, int* outcount,
			struct bmi_method_unexpected_info* info,
			int max_idle_time_ms)
{
	int ret = 0;
	if(portals_node_type == SERVER)
	{
		int i = 0;
		/* TODO make these global per server... prevent alloc / dealloc */
		void ** buffers = (void **)bmiptl_safe_malloc(sizeof(void *) * incount, __FILE__, __func__, __LINE__);
		size_t * sizes = (size_t *)bmiptl_safe_malloc(sizeof(size_t) * incount, __FILE__, __func__, __LINE__);
		int64_t * tags = (int64_t *)bmiptl_safe_malloc(sizeof(int64_t) * incount, __FILE__, __func__, __LINE__);
		ptl_process_id_t * addrs = (ptl_process_id_t *)bmiptl_safe_malloc(sizeof(ptl_process_id_t) * incount, __FILE__, __func__, __LINE__);

		*outcount = bmip_server_test_unex_events(max_idle_time_ms, incount, buffers, sizes, tags, addrs);

		/* iterate over all of the pending unex msgs */
		for(i = 0 ; i < *outcount ; i++)
		{
			info[i].error_code = 0;
			info[i].addr = bmip_lookup_addr(addrs[i]);
			info[i].buffer = buffers[i];
			info[i].size = (bmi_size_t)sizes[i];
			info[i].tag = (bmi_msg_tag_t)tags[i];
		}

		/* TODO make these global per server... prevent alloc / dealloc */
		/* cleanup */
		bmiptl_safe_free(buffers, __FILE__, __func__, __LINE__);
		bmiptl_safe_free(sizes, __FILE__, __func__, __LINE__);
		bmiptl_safe_free(tags, __FILE__, __func__, __LINE__);
		bmiptl_safe_free(addrs, __FILE__, __func__, __LINE__);

		return 0;
	}
	/* not supported on clients */
	else
	{
		return -1;
	}
	return ret;
}

/* Invoked on BMI_addr_lookup, also part of BMI_intialize. */
static struct bmi_method_addr *
BMI_portals_method_addr_lookup(const char * id)
{
	int found = 0;
	bmi_method_addr_p addr = NULL;
	portals_addr_t * a = NULL, * n_a = NULL;

	//fprintf(stderr, "%s:%i id = %s\n", __func__, __LINE__, id);

	/* portals://nid00000:000 */

	/* check that this ID has the portals prefix */
	if (strncmp(id, "portals://", 10))
		return NULL;

	/* scan the list of known addrs for this addr */
	gen_mutex_lock(&addr_lock);
	qlist_for_each_entry_safe(a, n_a, &bmip_addr_list, list)
	{
		/* if we found a match */
		if(strcmp(id, a->hostname) == 0)
		{
			found = 1;
			addr = a->p_addr; /* store the address */
			break;
		}
	}

	/* if we did not find the address, make a new one and add it to the list */
	if(!found)
	{
		if(!(addr = bmi_alloc_method_addr(portals_method_id,
						   sizeof(portals_addr_t))))
		{
			gen_mutex_unlock(&addr_lock);
			return NULL;
		}

		char nid[6];
		char pid[4];

		/* init the addr data */
		memset(nid, 0, 6);
		memset(pid, 0, 4);

		/* copy the addr info */
		memcpy(&nid[0], &(id[13]), 5);
		memcpy(&pid[0], &(id[19]), 3);

		/* store the pid and nid */
		((portals_addr_t *)addr->method_data)->pid.nid = atoi(nid);
		((portals_addr_t *)addr->method_data)->pid.pid = atoi(pid);
		((portals_addr_t *)addr->method_data)->p_addr = addr;

		/* store a copy of the hostname */
		((portals_addr_t *)addr->method_data)->hostname = strdup(id);

		/* add it to the list */
		qlist_add_tail(&((portals_addr_t *)addr->method_data)->list, &bmip_addr_list);
	}
	gen_mutex_unlock(&addr_lock);

	return addr;
}

/* Invoked on BMI_post_send_list.  */
static int
BMI_portals_post_send_list(bmi_op_id_t* id, bmi_method_addr_p dest,
			const void*const* buffer_list,
			const bmi_size_t* size_list, int list_count,
			bmi_size_t total_size, enum bmi_buffer_type buffer_type,
			bmi_msg_tag_t tag, void* user_ptr,
			bmi_context_id context_id, PVFS_hint hints)
{
	int ret = 0;
	if(portals_node_type == SERVER)
	{
		method_op_p mop = bmi_alloc_method_op(0);
		*id = mop->op_id;
		bmip_server_post_send(((portals_addr_t *)dest->method_data)->pid, (int64_t)tag, list_count, (void **)buffer_list, (size_t *)size_list, BMIP_USE_CVTEST, user_ptr, *id);
		return 0;
	}
	else
	{
		bmip_delegate_pid = ((portals_addr_t *)dest->method_data)->pid;
		bmip_delegate_list_count = list_count;
		bmip_delegate_buffer_list = buffer_list;
		bmip_delegate_size_list = size_list;
		bmip_delegate_tag = tag;
		bmip_delegate_ret = 0;
		bmip_client_op_type = BMIP_POSTSENDLIST;

		bmip_comm_barrier();
		bmip_comm_barrier();

		return bmip_delegate_ret;
	}
	return -1;
}

static void
BMI_portals_client_post_send_list(void)
{
	bmip_client_send(bmip_delegate_pid, bmip_delegate_list_count, (void **)bmip_delegate_buffer_list, (size_t *)bmip_delegate_size_list, (int64_t)bmip_delegate_tag);
	bmip_delegate_ret = 1;
}

/* Invoked on BMI_post_recv_list.  */
static int
BMI_portals_post_recv_list(bmi_op_id_t* id, bmi_method_addr_p src,
			void *const* buffer_list, const bmi_size_t* size_list,
			int list_count, bmi_size_t total_expected_size,
			bmi_size_t* total_actual_size,
			enum bmi_buffer_type buffer_type, bmi_msg_tag_t tag,
			void* user_ptr, bmi_context_id context_id,
			PVFS_hint hints)
{
	int ret = 0;
	if(portals_node_type == SERVER)
	{
		method_op_p mop = bmi_alloc_method_op(0);
		*id = mop->op_id;
		bmip_server_post_recv(((portals_addr_t *)src->method_data)->pid, (int64_t)tag, list_count, (void **)buffer_list, (size_t *)size_list, BMIP_USE_CVTEST, user_ptr, *id);
		*total_actual_size = total_expected_size;
		return 0;
	}
	else
	{
                bmip_delegate_pid = ((portals_addr_t *)src->method_data)->pid;
                bmip_delegate_list_count = list_count;
                bmip_delegate_buffer_list = buffer_list;
                bmip_delegate_size_list = size_list;
                bmip_delegate_tag = tag;
                bmip_delegate_ret = 0;
		bmip_client_op_type = BMIP_POSTRECVLIST;

		bmip_comm_barrier();
		bmip_comm_barrier();

		*total_actual_size = total_expected_size;

		return bmip_delegate_ret;
	}
	return -1;
}

static void
BMI_portals_client_post_recv_list(void)
{
        bmip_client_recv(bmip_delegate_pid, bmip_delegate_list_count, (void **)bmip_delegate_buffer_list, (size_t *)bmip_delegate_size_list, (int64_t)bmip_delegate_tag);
        bmip_delegate_ret = 1;
}

static int
BMI_portals_post_sendunexpected_list(bmi_op_id_t* id, bmi_method_addr_p dest,
				  const void*const* buffer_list,
				  const bmi_size_t* size_list, int list_count,
				  bmi_size_t total_size,
				  enum bmi_buffer_type buffer_type,
				  bmi_msg_tag_t tag, void* user_ptr,
				  bmi_context_id context_id, PVFS_hint hints)
{
	return -1;
}

/* Invoked on BMI_open_context.  We only support one, global context.  */
static int
BMI_portals_open_context(bmi_context_id context_id)
{
	return 0;
}

/* Invoked on BMI_close_context.  We only support one, global context.  */
static void
BMI_portals_close_context(bmi_context_id context_id)
{
	return;
}

/* Invoked on BMI_cancel.  */
static int
BMI_portals_cancel(bmi_op_id_t id, bmi_context_id context_id)
{
	/* only need to cancel the server ops */
	if(portals_node_type == SERVER)
	{
		method_op_p op = (method_op_p)id_gen_fast_lookup(id);
		op->error_code = BMI_ECANCEL;
		bmi_dealloc_method_op(op);

		/* TODO scan for ops in the bmip layer and cancel them */
	}

	return 0;
}

/* Invoked on BMI_rev_lookup_unexpected.  */
static const char*
BMI_portals_rev_lookup_unexpected(bmi_method_addr_p map)
{
	return ((portals_addr_t *)map->method_data)->hostname;
}

const struct bmi_method_ops bmi_portals_ops =
{
	.method_name = "bmi_portals",

	.flags = BMI_METHOD_FLAG_NO_POLLING,

	.initialize = BMI_portals_initialize,
	.finalize = BMI_portals_finalize,

	.set_info = BMI_portals_set_info,
	.get_info = BMI_portals_get_info,

	.memalloc = BMI_portals_memalloc,
	.memfree = BMI_portals_memfree,
	.unexpected_free = BMI_portals_unexpected_free,

	.post_send = BMI_portals_post_send,
	.post_sendunexpected = BMI_portals_post_sendunexpected,
	.post_recv = BMI_portals_post_recv,

	.test = BMI_portals_test,
	.testsome = BMI_portals_testsome,
	.testcontext = BMI_portals_testcontext,
	.testunexpected = BMI_portals_testunexpected,

	.method_addr_lookup = BMI_portals_method_addr_lookup,

	.post_send_list = BMI_portals_post_send_list,
	.post_recv_list = BMI_portals_post_recv_list,
	.post_sendunexpected_list = BMI_portals_post_sendunexpected_list,

	.open_context = BMI_portals_open_context,
	.close_context = BMI_portals_close_context,

	.cancel = BMI_portals_cancel,

	.rev_lookup_unexpected = BMI_portals_rev_lookup_unexpected,
};
