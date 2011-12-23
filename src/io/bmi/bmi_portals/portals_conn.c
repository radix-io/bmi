#include "portals_conn.h"

#include "portals_comm.h"
#include "portals_wrappers.h"
#include "portals_helpers.h"
#include "portals_trace.h"

#include <portals/portals3.h>
#include <sys/utsname.h>

#include <src/common/gen-locks/gen-locks.h> 
#include <src/common/quicklist/quicklist.h> 

#include <time.h>
#include <errno.h>

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#define _GNU_SOURCE
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/mman.h>

#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sched.h>

#include "dlmalloc.h"

/* consts and constraints */
#define BMIP_EQ_SIZE (1<<16) 

#define BMIP_MAX_TEST_COUNT 1000

/* local connection and ID data */
static ptl_handle_ni_t local_ni = PTL_INVALID_HANDLE;
static ptl_process_id_t local_pid;
static ptl_process_id_t any_pid;
static ptl_process_id_t no_pid;

/* the EQs... one for expected messages and one for unexpected messages */
static ptl_handle_eq_t bmi_eq = PTL_INVALID_HANDLE;

static ptl_handle_me_t reject_me;
static ptl_handle_me_t mark_me;
static ptl_handle_me_t unex_me_handle;
static ptl_handle_md_t unex_md_handle;
static ptl_handle_me_t ex_me_handle;
static ptl_handle_me_t ex_req_me_handle;
static ptl_handle_me_t ex_rsp_me_handle;
static ptl_handle_md_t ex_md_handle;
static ptl_handle_md_t ex_req_md_handle;
static ptl_handle_md_t ex_rsp_md_handle;

/* counters */
static uint64_t ex_send_count = 0;
static uint64_t ex_send_size = 0;
static double ex_send_time = 0;
static uint64_t ex_recv_count = 0;
static uint64_t ex_recv_size = 0;
static double ex_recv_time = 0;
static uint64_t unex_send_count = 0;
static uint64_t unex_send_size = 0;
static double unex_send_time = 0;

/* local unex msg space... 8KB */
#define BMIP_CLIENT_UNEX_SPACE (8 * (1<<10))
#define BMIP_SERVER_UNEX_SPACE (8 * (1<<10))
#define BMIP_UNEX_MSG_SIZE (8 * (1<<10) + 1)
static void * bmip_unex_space = NULL;
static void * bmip_unex_space_end = NULL;
static gen_mutex_t bmip_server_unex_space_mutex = GEN_MUTEX_INITIALIZER;
static size_t bmip_unex_space_length = 0;

/* ex msg space */
#define BMIP_SERVER_EX_SPACE (2ul * 1024ul * 1024ul * 1024ul)
#define BMIP_CLIENT_EX_SPACE (16 * (1<<20))
#define BMIP_EX_MSG_SIZE (8 * (1<<20) + 1)
static void * bmip_ex_space = NULL;
static void * bmip_ex_space_end = NULL;
 
static size_t bmip_ex_space_length = 0;

/* request space... */
static size_t bmip_ex_req_space[BMIP_MAX_LISTIO]; 
static const size_t bmip_ex_req_space_length = sizeof(bmip_ex_req_space);

/* response space... */
static size_t bmip_ex_rsp_space[BMIP_MAX_LISTIO]; 
static const size_t bmip_ex_rsp_space_length = sizeof(bmip_ex_rsp_space);

/* match bit flags for the various bmi operations */
static const uint64_t unex_mb = (1ULL << 63);
static const uint64_t ex_mb = (1ULL << 62);
static const uint64_t ex_req_mb = (1ULL << 61);
static const uint64_t ex_rsp_mb = (1ULL << 60);
static const uint64_t ex_req_put_mb = (1ULL << 59);
static const uint64_t ex_req_get_mb = (1ULL << 58);
static const uint64_t unex_server_traffic_mb = (1ULL << 57);

/* counters for the done ops */
static int server_num_done = 0;
static int server_num_unex_done = 0;

/* pending and done operation lists */
static QLIST_HEAD(bmip_unex_pending);
static QLIST_HEAD(bmip_unex_ss_pending);
static QLIST_HEAD(bmip_unex_done);
static QLIST_HEAD(bmip_cur_ops);
static QLIST_HEAD(bmip_cur_ss_ops);
static QLIST_HEAD(bmip_done_ops);
static QLIST_HEAD(bmip_server_pending_send_ops);
static QLIST_HEAD(bmip_server_pending_recv_ops);
static QLIST_HEAD(bmip_addr_seq_list);

static int bmip_s2s_exsend_active = 0;
static QLIST_HEAD(bmip_s2s_exsend);
static int bmip_s2s_unexsend_active = 0;
static QLIST_HEAD(bmip_s2s_unexsend);
static QLIST_HEAD(bmip_precvlocalop);
static QLIST_HEAD(bmip_psendlocalop);

/* lock for list accesses */
static gen_mutex_t list_mutex = GEN_MUTEX_INITIALIZER;
static gen_mutex_t sig_mutex = GEN_MUTEX_INITIALIZER;

/* attr when we use barrier sync */
static pthread_barrierattr_t bmip_context_attr;

/* shutdown flag */
static int bmip_shutdown = 0;

/* self pipe data structures */
static fd_set workfdset;
static int workfdmax = -1;
static int workfds[2];
static int unexworkfds[2];

#ifdef BMIP_TRACE
#define BMIP_EV_TRACE_LIMIT (64ul * 1024ul * 1024ul)
static int bmip_server_events[BMIP_EV_TRACE_LIMIT];
static int64_t bmip_server_mb[BMIP_EV_TRACE_LIMIT];
static int bmip_server_pid[BMIP_EV_TRACE_LIMIT];
static int bmip_server_nid[BMIP_EV_TRACE_LIMIT];
static int bmip_server_events_counter = 0;
static int bmip_client_events[BMIP_EV_TRACE_LIMIT];
static int64_t bmip_client_mb[BMIP_EV_TRACE_LIMIT];
static int bmip_client_pid[BMIP_EV_TRACE_LIMIT];
static int bmip_client_nid[BMIP_EV_TRACE_LIMIT];
static int bmip_client_handler[BMIP_EV_TRACE_LIMIT];
static int bmip_client_events_counter = 0;
#endif

static unsigned int bmip_client_seq = 0;

/* portals addr index... this never changes! */
#define BMIP_PTL_INDEX 37

/* server data allocation space */
static mspace portals_data_space = NULL;

static size_t bmip_total_alloc_space = 0;
static size_t bmip_total_alloc_count = 0;

static void * bmip_safe_malloc(size_t size, char * fl, char * fn, int l)
{
	void * m = NULL;

	if(size == 0)
	{
		fprintf(stderr, "%s:%i WARNING: 0 size detected. source info %s:%s:%i\n", __func__, __LINE__, fl, fn, l);
		fflush(stderr);
		return NULL;
	}
	
	m = malloc(size);
	if(!m)
	{
		fprintf(stderr, "%s:%i malloc failed. size = %lu\n", __func__, __LINE__, size);
		assert(m != NULL);
	}
	
	bmip_total_alloc_space += size;
	bmip_total_alloc_count++;

	return m;
}

static void bmip_safe_free(void * m, char * fl, char * fn, int l)
{
	if(!m)
	{
		fprintf(stderr, "%s:%i WARNING: NULL buffer detected. source info %s:%s:%i\n", __func__, __LINE__, fl, fn, l);
		fflush(stderr);
		return;
	}
	free(m);

	bmip_total_alloc_count--;
}

void bmip_allocate_client_mem(void)
{
	bmip_ex_space_length = BMIP_CLIENT_EX_SPACE;
	bmip_ex_space = (void *)bmip_safe_malloc(sizeof(char) * BMIP_CLIENT_EX_SPACE, __FILE__, __func__, __LINE__);
	bmip_unex_space_length = BMIP_CLIENT_UNEX_SPACE;
	bmip_unex_space = (void *)bmip_safe_malloc(sizeof(char) * BMIP_CLIENT_UNEX_SPACE, __FILE__, __func__, __LINE__);
}

void bmip_allocate_server_mem(void)
{
	bmip_ex_space_length = BMIP_SERVER_EX_SPACE;
	bmip_ex_space = (void *)bmip_safe_malloc(sizeof(char) * BMIP_SERVER_EX_SPACE, __FILE__, __func__, __LINE__);
	bmip_unex_space_length = BMIP_SERVER_UNEX_SPACE;
	bmip_unex_space = (void *)bmip_safe_malloc(sizeof(char) * BMIP_SERVER_UNEX_SPACE, __FILE__, __func__, __LINE__);
}

void bmip_free_server_mem(void)
{
	bmip_safe_free(bmip_ex_space, __FILE__, __func__, __LINE__);
	bmip_ex_space_length = 0;
	bmip_ex_space = NULL;
	bmip_safe_free(bmip_unex_space, __FILE__, __func__, __LINE__);
	bmip_unex_space_length = 0;
	bmip_unex_space = NULL;
}

void bmip_free_client_mem(void)
{
	bmip_safe_free(bmip_ex_space, __FILE__, __func__, __LINE__);
	bmip_ex_space_length = 0;
	bmip_ex_space = NULL;
	bmip_safe_free(bmip_unex_space, __FILE__, __func__, __LINE__);
	bmip_unex_space_length = 0;
	bmip_unex_space = NULL;
}

char bmip_addr_is_server(ptl_process_id_t pid)
{
	char s = 0;

	bmip_seq_t * n = NULL, * nsafe = NULL;
	qlist_for_each_entry_safe(n, nsafe, &bmip_addr_seq_list, list)
        {
		if(n->target.pid == pid.pid && n->target.nid == pid.nid)
		{
			s = n->server;
			if(s == 2)
			{
				fprintf(stderr, "%s:%i unkown target type... ERROR\n", __func__, __LINE__);
				exit(-1);
			}
			break;
		}
	}

	return s;
}

unsigned int bmip_get_addr_seq(ptl_process_id_t pid, char server)
{
	int found = 0;
	unsigned int s = 0;

	bmip_seq_t * n = NULL, * nsafe = NULL;
	qlist_for_each_entry_safe(n, nsafe, &bmip_addr_seq_list, list)
        {
		if(n->target.pid == pid.pid && n->target.nid == pid.nid)
		{
			if(n->server == 2)
			{
				n->server = server;
			}	
			found = 1;
			n->counter++;
			s = n->counter;
			break;
		}
	}

	/* if we did not find the address, make a new seq number for it */
	if(!found)
	{
		bmip_seq_t * n = bmip_safe_malloc(sizeof(bmip_seq_t), __FILE__, __func__, __LINE__);
		n->counter = 0;
		n->server = server;
		s = n->counter;
		n->target.pid = pid.pid;
		n->target.nid = pid.nid;
		qlist_add_tail(&n->list, &bmip_addr_seq_list);
	}

	return 0;
}

int bmip_get_max_ex_msg_size(void)
{
	return BMIP_EX_MSG_SIZE;
}

int bmip_get_max_unex_msg_size(void)
{
	return BMIP_UNEX_MSG_SIZE;
}

void bmip_monitor_shutdown(void)
{
	bmip_shutdown = 1;
}

void bmip_data_init(size_t len)
{
	bmip_ex_space_end = bmip_ex_space + len;

        portals_data_space = create_mspace_with_base(bmip_ex_space, len, 1);
}

int bmip_data_destroy(void)
{
        destroy_mspace(portals_data_space);

        bmip_ex_space = NULL;
        bmip_ex_space_end = NULL;
        portals_data_space = NULL;

        return 0;
}

void * bmip_data_get_base(void)
{
        return bmip_ex_space;
}

void * bmip_data_get(size_t len)
{
        return mspace_malloc(portals_data_space, len);
}

void bmip_data_release(void * mem)
{
        mspace_free(portals_data_space, mem);
}

size_t bmip_data_get_offset(void * buffer)
{
        return (size_t) (buffer - bmip_ex_space);
}

bmip_portals_conn_op_t * bmip_ss_unex_pending(void)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_s2s_unexsend, list)
	{
		found = 1;
		break;
	}

	if(!found)
		n = NULL;
	else
        	qlist_del(&n->list);

	return n;
}

bmip_portals_conn_op_t * bmip_ss_ex_pending(void)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_s2s_exsend, list)
	{
		found = 1;
		break;
	}

	if(!found)
		n = NULL;
	else
        	qlist_del(&n->list);

	return n;
}

bmip_portals_conn_op_t * bmip_locate_precvlocalop(bmip_portals_conn_op_t * sop)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_precvlocalop, list)
	{
		if(n->match_bits == sop->match_bits && ((sop->target.pid == n->target.pid && sop->target.nid == n->target.nid)))
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;
	else
        	qlist_del(&n->list);

	return n;
}

bmip_portals_conn_op_t * bmip_locate_psendlocalop(bmip_portals_conn_op_t * rop)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_psendlocalop, list)
	{
		if(n->match_bits == rop->match_bits && ((rop->target.pid == n->target.pid && rop->target.nid == n->target.nid)))
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;
	else
        	qlist_del(&n->list);

	return n;
}

bmip_portals_conn_op_t * bmip_ss_op_in_progress(int64_t mb, ptl_process_id_t pid)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_cur_ss_ops, list)
	{
		if(n->match_bits == mb && ((pid.pid == n->target.pid && pid.nid == n->target.nid)))
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;

	return n;
}

bmip_portals_conn_op_t * bmip_op_in_progress(int64_t mb, ptl_process_id_t pid)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_cur_ops, list)
	{
		if(n->match_bits == mb && ((pid.pid == n->target.pid && pid.nid == n->target.nid)))
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;

	return n;
}

bmip_portals_conn_op_t * bmip_unex_ss_op_in_progress(int64_t mb, ptl_process_id_t pid)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_unex_ss_pending, list)
	{
		if(n->match_bits == mb && ((pid.pid == n->target.pid && pid.nid == n->target.nid)))
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;

	return n;
}

bmip_portals_conn_op_t * bmip_unex_op_in_progress(int64_t mb, ptl_process_id_t pid)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	qlist_for_each_entry_safe(n, nsafe, &bmip_unex_pending, list)
	{
		if(n->match_bits == mb && ((pid.pid == n->target.pid && pid.nid == n->target.nid)))
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;

	return n;
}

bmip_portals_conn_op_t * bmip_server_send_pending(ptl_process_id_t target, int64_t match_bits)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	/* if the addr is the local server addr, return NULL since we ignore the event */
	if(bmip_is_local_addr(target))
		return NULL;

	qlist_for_each_entry_safe(n, nsafe, &bmip_server_pending_send_ops, list)
	{
		if(target.pid == n->target.pid && target.nid == n->target.nid && match_bits == n->match_bits)
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;

	return n;
}

bmip_portals_conn_op_t * bmip_server_recv_pending(ptl_process_id_t target, int64_t match_bits)
{
	bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
	int found = 0;

	/* if the addr is the local server addr, return NULL since we ignore the event */
	if(bmip_is_local_addr(target))
		return NULL;

	qlist_for_each_entry_safe(n, nsafe, &bmip_server_pending_recv_ops, list)
	{
		if(target.pid == n->target.pid && target.nid == n->target.nid && match_bits == n->match_bits)
		{
			found = 1;
			break;
		}
	}

	if(!found)
		n = NULL;

	return n;
}

int bmip_is_local_addr(ptl_process_id_t pid)
{
	if(local_pid.pid == pid.pid && local_pid.nid == pid.nid)
		return 1;
	return 0;
}

void bmip_reinit_sighandler(void)
{
        FD_ZERO(&workfdset);
        FD_SET(workfds[0], &workfdset);
        FD_SET(unexworkfds[0], &workfdset);
}

int bmip_init_comm(void)
{
	int ret = 0;
	int num_interfaces = 0;

	/* init the pipe notify */

	/* ex msg self pipe */
	pipe(workfds);
	FD_ZERO(&workfdset);
	FD_SET(workfds[0], &workfdset);
	if(workfds[0] > workfdmax)
		workfdmax = workfds[0];

	/* unex msg self pipe */
	pipe(unexworkfds);
	FD_SET(unexworkfds[0], &workfdset);
	if(unexworkfds[0] > workfdmax)
		workfdmax = unexworkfds[0];

	/* init portals */ 
	ret = bmip_ptl_init(&num_interfaces);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not initialize portals", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* setup the context barrier attr */
	pthread_barrierattr_setpshared(&bmip_context_attr, PTHREAD_PROCESS_SHARED);
out:
	return ret;
}

int bmip_init_comm_ni(int pid)
{
	int ret = 0;

	/* if the ni is already initialized */
	if (local_ni != PTL_INVALID_HANDLE)
	{
		goto out;
	}

	/* setup the any pid */
	any_pid.nid = PTL_NID_ANY;
	any_pid.pid = PTL_PID_ANY;

	/* setup no pid */
	no_pid.nid = 0;
	no_pid.pid = 0;

	/* initialize the local ni on the any_pid addr */
	if(pid == -1)
	{
		ret = bmip_ptl_ni_init(IFACE_FROM_BRIDGE_AND_NALID(PTL_BRIDGE_UK,PTL_IFACE_SS), any_pid.pid, NULL, NULL, &local_ni);
		if(ret != PTL_OK)
		{
			bmip_fprintf("could not initialize ni", __FILE__, __func__, __LINE__);
			goto out;
		}
	}
	else
	{
		ret = bmip_ptl_ni_init(IFACE_FROM_BRIDGE_AND_NALID(PTL_BRIDGE_UK,PTL_IFACE_SS), pid, NULL, NULL, &local_ni);
		if(ret != PTL_OK)
		{
			bmip_fprintf("could not initialize ni", __FILE__, __func__, __LINE__);
			goto out;
		}
	}
	local_pid = bmip_get_ptl_id();
	bmip_trace_set_local_addr(local_pid);
	ptl_uid_t uid;

	ret = PtlGetUid(local_ni, &uid); 
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not get the uid", __FILE__, __func__, __LINE__);
		goto out;
	}
out:
	return ret;
}

int bmip_dest_comm_ni(void)
{
	int ret = 0;

	/* if handle is invalid, do not try to shutdown */
	if (local_ni == PTL_INVALID_HANDLE)
	{
		goto out;
	}

	/* shutdown portals */
	ret = bmip_ptl_ni_fini(local_ni);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not finalize ni", __FILE__, __func__, __LINE__);
		goto out;
	}

out:
	return ret;
}

ptl_process_id_t bmip_get_ptl_id(void)
{
	int ret = 0;
	ptl_process_id_t id;

	/* get the local ID. if one could not be found, return an invalid handle */
	ret = bmip_ptl_get_id(local_ni, &id);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not get id from ni", __FILE__, __func__, __LINE__);
		local_ni = PTL_INVALID_HANDLE;
		goto out;
	}
out:
	return id; 
}

int bmip_setup_unex_eq(void)
{
	int ret = 0;
	ptl_md_t md;

	/* setup the unexpected buffer space */
	md.start = &bmip_unex_space[0];	
	md.length = bmip_unex_space_length;
	md.threshold = PTL_MD_THRESH_INF;
	md.user_ptr = NULL;
	md.options = PTL_MD_OP_GET | PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	md.eq_handle = bmi_eq;

	/* insert the ME */
	ret = bmip_ptl_me_insert(reject_me, any_pid, unex_mb, (0x7fffffffULL << 32) | 0xffffffffULL, PTL_RETAIN, PTL_INS_BEFORE, &unex_me_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not insert unex me", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* attach the md to the unex me */
	ret = bmip_ptl_md_attach(unex_me_handle, md, PTL_RETAIN, &unex_md_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not attach unex md", __FILE__, __func__, __LINE__);
		goto out;
	}
out:
	return ret;
}

int bmip_setup_ex_eq(int is_server)
{
        int ret = 0;
        ptl_md_t md;

	/* create the ex msg space */
	if(is_server)
	{
		bmip_ex_space_length = BMIP_SERVER_EX_SPACE;
		bmip_data_init(BMIP_SERVER_EX_SPACE);
	}
	else
	{
		bmip_ex_space_length = BMIP_CLIENT_EX_SPACE;
		bmip_data_init(BMIP_CLIENT_EX_SPACE);
	}

        /* setup the expected buffer space */
        md.start = bmip_ex_space;
        md.length = bmip_ex_space_length;
        md.threshold = PTL_MD_THRESH_INF;
        md.user_ptr = NULL;
        md.options = PTL_MD_OP_PUT | PTL_MD_OP_GET | PTL_MD_MANAGE_REMOTE;
        md.eq_handle = bmi_eq;

        /* insert the ME */
        ret = bmip_ptl_me_insert(reject_me, any_pid, ex_mb, (0x7fffffffULL << 32) | 0xffffffffULL, PTL_RETAIN, PTL_INS_BEFORE, &ex_me_handle);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not insert ex me", __FILE__, __func__, __LINE__);
                goto out;
        }

        /* attach the md to the ex me */
        ret = bmip_ptl_md_attach(ex_me_handle, md, PTL_RETAIN, &ex_md_handle);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not attach ex md", __FILE__, __func__, __LINE__);
                goto out;
        }

        /* setup the req expected buffer space */
        md.start = &bmip_ex_req_space[0];
        md.length = bmip_ex_req_space_length;
        md.threshold = PTL_MD_THRESH_INF;
        md.user_ptr = NULL;
        md.options = PTL_MD_OP_PUT | PTL_MD_OP_GET | PTL_MD_MANAGE_REMOTE;
        md.eq_handle = bmi_eq;

        /* insert the ME */
        ret = bmip_ptl_me_insert(mark_me, any_pid, ex_req_mb, 0xffffffffULL, PTL_RETAIN, PTL_INS_BEFORE, &ex_req_me_handle);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not insert ex req me", __FILE__, __func__, __LINE__);
                goto out;
        }

        /* attach the md to the ex me */
        ret = bmip_ptl_md_attach(ex_req_me_handle, md, PTL_RETAIN, &ex_req_md_handle);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not attach ex req md", __FILE__, __func__, __LINE__);
                goto out;
        }

        /* setup the rsp expected buffer space */
        md.start = &bmip_ex_rsp_space[0];
        md.length = bmip_ex_rsp_space_length;
        md.threshold = PTL_MD_THRESH_INF;
        md.user_ptr = NULL;
        md.options = PTL_MD_OP_PUT | PTL_MD_OP_GET | PTL_MD_MANAGE_REMOTE;
        md.eq_handle = bmi_eq;

        /* insert the ME */
        ret = bmip_ptl_me_insert(mark_me, any_pid, ex_rsp_mb, 0xffffffffULL, PTL_RETAIN, PTL_INS_BEFORE, &ex_rsp_me_handle);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not insert ex rsp me", __FILE__, __func__, __LINE__);
                goto out;
        }

        /* attach the md to the ex me */
        ret = bmip_ptl_md_attach(ex_rsp_me_handle, md, PTL_RETAIN, &ex_rsp_md_handle);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not attach ex rsp md", __FILE__, __func__, __LINE__);
                goto out;
	}

out:
        return ret;
}

int bmip_dest_unex_eq(void)
{
	int ret = 0;

	ret = bmip_ptl_me_unlink(unex_me_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not attach unex md", __FILE__, __func__, __LINE__);
		goto out;
	}
out:
	return ret;
}

int bmip_dest_ex_eq(void)
{
	int ret = 0;

	ret = bmip_ptl_me_unlink(ex_me_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not attach unex md", __FILE__, __func__, __LINE__);
		goto out;
	}
out:
	return ret;
}

int bmip_setup_eqs(int is_server)
{
	int ret = 0;

	/* allocate the unex eq */
	ret = bmip_ptl_eq_alloc(local_ni, BMIP_EQ_SIZE, PTL_EQ_HANDLER_NONE, &bmi_eq);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not allocate unexpected eq", __FILE__, __func__, __LINE__);
		goto out;
	}

	ptl_process_id_t no_pid;
    	no_pid.nid = 0;
    	no_pid.pid = 0;

	/* add the mark me */
    	ret = PtlMEAttach(local_ni, BMIP_PTL_INDEX, no_pid, 0, 0, PTL_RETAIN, PTL_INS_BEFORE, &mark_me);
    	if(ret)
	{
		bmip_fprintf("could not setup reject me", __FILE__, __func__, __LINE__);
        	goto out;
    	}

	/* create the base / reject me */
    	ret = bmip_ptl_me_attach(local_ni, BMIP_PTL_INDEX, any_pid, 0, (0x7fffffffULL << 32) | 0xffffffffULL, PTL_RETAIN, PTL_INS_AFTER, &reject_me);
    	if(ret != PTL_OK)
	{
		bmip_fprintf("could not setup reject me", __FILE__, __func__, __LINE__);
        	goto out;
	}

	/* setup the unex eq, me, and md */
	ret = bmip_setup_unex_eq();
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not setup unex eq", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* setup the ex eq, me, and md */
	ret = bmip_setup_ex_eq(is_server);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not setup ex eq", __FILE__, __func__, __LINE__);
		goto out;
	}
out:
	return ret;
}

int bmip_dest_eqs(void)
{
	int ret = 0;

	ret = bmip_dest_unex_eq();
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not destroy unex eq reosurces", __FILE__, __func__, __LINE__);
		goto out;
	}

	ret = bmip_ptl_eq_free(bmi_eq);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not free unexpected eq", __FILE__, __func__, __LINE__);
		goto out;
	}

	ret = bmip_dest_ex_eq();
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not destroy unex eq reosurces", __FILE__, __func__, __LINE__);
		goto out;
	}

out:
	return ret;
}

int bmip_setup_ac(void)
{
	return 0;
}

int bmip_init(int pid)
{
	int ret = 0;

	/* init portals */
	ret = bmip_init_comm();
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not init the comm", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* init the portals local interface */
	ret = bmip_init_comm_ni(pid);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not init the ni", __FILE__, __func__, __LINE__);
		goto out;
	}

	ret = bmip_setup_ac();
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not setup the ac", __FILE__, __func__, __LINE__);
		goto out;
	}

out:
	return ret; 
}

int bmip_finalize(void)
{
	int ret = 0;

	ret = bmip_dest_comm_ni();
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not destroy the ni", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* init the mem pool */
	bmip_data_destroy();
out:	
	return ret;
}

int bmip_get_ptl_nid(void)
{
	return (bmip_get_ptl_id()).nid;
}

int bmip_get_ptl_pid(void)
{
	return (bmip_get_ptl_id()).pid;
}

int bmip_wait_unex_event(ptl_event_t * ev)
{
	int ret = 0;

	ret = bmip_wait_event(0, &bmi_eq, ev);
	if(ret == -1)
	{
		bmip_fprintf("could not wait for an unex event", __FILE__, __func__, __LINE__);
		goto out;
	}
out:
	return ret;
}

int bmip_wait_ex_event(ptl_event_t * ev)
{
        int ret = 0;

        ret = bmip_wait_event(0, &bmi_eq, ev);
        if(ret == -1)
        {
                bmip_fprintf("could not wait for an ex event", __FILE__, __func__, __LINE__);
                goto out;
        }
out:
        return ret;
}

int bmip_wait_local_get(struct qlist_head * pelist)
{
	int ecount = 0;
	int etype = 0;
	int i = 0;

        bmip_pending_event_t * n = NULL, * nsafe = NULL;
        qlist_for_each_entry_safe(n, nsafe, pelist, list)
        {
		if(n->eventid == PTL_EVENT_SEND_END)
                {
                        qlist_del(&n->list);
			bmip_safe_free(n, __FILE__, __func__, __LINE__);
                        ecount++;
                }
		else if(n->eventid == PTL_EVENT_REPLY_END)
                {
                        qlist_del(&n->list);
			bmip_safe_free(n, __FILE__, __func__, __LINE__);
                        ecount++;
                }
		if(ecount == 2)
                       	break;
        }

	while(ecount < 2)
	{
		ptl_event_t ev;
		etype = bmip_wait_ex_event(&ev);

#ifdef BMIP_TRACE
		/* track all of the events */
                if(bmip_client_events_counter >= BMIP_EV_TRACE_LIMIT)
                        bmip_client_events_counter = 0;
                int eindex = bmip_client_events_counter++;
                bmip_client_events[eindex] = etype;
                bmip_client_pid[eindex] = ev.initiator.pid;
                bmip_client_nid[eindex] = ev.initiator.nid;
                bmip_client_mb[eindex] = ev.match_bits;
                bmip_client_handler[eindex] = 1;
#endif
		if(etype == PTL_EVENT_SEND_END)
		{
			ecount++;
		}
		else if(etype == PTL_EVENT_REPLY_END)
		{
			ecount++;
		}
		else
		{
			bmip_pending_event_t * pe = (bmip_pending_event_t *) bmip_safe_malloc(sizeof(bmip_pending_event_t), __FILE__, __func__, __LINE__);
			pe->eventid = etype;
			pe->event = ev;
			qlist_add_tail(&pe->list, pelist);
		}
	}
	return 0;
}

int bmip_wait_unex_remote_get(struct qlist_head * pelist)
{
	int ecount = 0;
	int etype = 0;
        int i = 0;

        bmip_pending_event_t * n = NULL, * nsafe = NULL;
        qlist_for_each_entry_safe(n, nsafe, pelist, list)
        {
                if(n->eventid == PTL_EVENT_GET_END)
                {
                        qlist_del(&n->list);
			bmip_safe_free(n, __FILE__, __func__, __LINE__);
                        ecount++;
                }
                if(ecount == 1)
                        break;
        }

        while(ecount < 1)
        {
		ptl_event_t ev;
                etype = bmip_wait_unex_event(&ev);

                /* track all of the events */
#ifdef BMIP_TRACE
                if(bmip_client_events_counter >= BMIP_EV_TRACE_LIMIT)
                        bmip_client_events_counter = 0;
                int eindex = bmip_client_events_counter++;
                bmip_client_events[eindex] = etype;
                bmip_client_pid[eindex] = ev.initiator.pid;
                bmip_client_nid[eindex] = ev.initiator.nid;
                bmip_client_mb[eindex] = ev.match_bits;
                bmip_client_handler[eindex] = 2;
#endif
                if(etype == PTL_EVENT_GET_END)
                {
                        ecount++;
                }
                else
                {
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)bmip_safe_malloc(sizeof(bmip_pending_event_t), __FILE__, __func__, __LINE__);
                        pe->eventid = etype;
                        pe->event = ev;
			qlist_add_tail(&pe->list, pelist);
                }
        }
	return 0;
}

int bmip_wait_remote_get(struct qlist_head * pelist)
{
	int ecount = 0;
	int etype = 0;
        int i = 0;

        bmip_pending_event_t * n = NULL, * nsafe = NULL;
        qlist_for_each_entry_safe(n, nsafe, pelist, list)
        {
                if(n->eventid == PTL_EVENT_GET_END)
                {
                        qlist_del(&n->list);
			bmip_safe_free(n, __FILE__, __func__, __LINE__);
                        ecount++;
                }
                if(ecount == 1)
                        break;
        }

        while(ecount < 1)
        {
		ptl_event_t ev;
                etype = bmip_wait_ex_event(&ev);

#ifdef BMIP_TRACE
                /* track all of the events */
                if(bmip_client_events_counter >= BMIP_EV_TRACE_LIMIT)
                        bmip_client_events_counter = 0;
                int eindex = bmip_client_events_counter++;
                bmip_client_events[eindex] = etype;
                bmip_client_pid[eindex] = ev.initiator.pid;
                bmip_client_nid[eindex] = ev.initiator.nid;
                bmip_client_mb[eindex] = ev.match_bits;
                bmip_client_handler[eindex] = 3;
#endif
                if(etype == PTL_EVENT_GET_END)
                {
                        ecount++;
                }
                else
                {
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)bmip_safe_malloc(sizeof(bmip_pending_event_t), __FILE__, __func__, __LINE__);
                        pe->eventid = etype;
                        pe->event = ev;
			qlist_add_tail(&pe->list, pelist);
                }
        }
	return 0;
}

int bmip_wait_local_put(struct qlist_head * pelist)
{
        int ecount = 0;
	int etype = 0;
        int i = 0;

        bmip_pending_event_t * n = NULL, * nsafe = NULL;
        qlist_for_each_entry_safe(n, nsafe, pelist, list)
        {
                if(n->eventid == PTL_EVENT_SEND_END)
                {
                        qlist_del(&n->list);
			bmip_safe_free(n, __FILE__, __func__, __LINE__);
                        ecount++;
                }
                else if(n->eventid == PTL_EVENT_ACK)
                {
                        qlist_del(&n->list);
			bmip_safe_free(n, __FILE__, __func__, __LINE__);
                        ecount++;
                }
                if(ecount == 2)
                        break;
        }

        while(ecount < 2)
        {
		ptl_event_t ev;
                etype = bmip_wait_ex_event(&ev);

#ifdef BMIP_TRACE
                /* track all of the events */
                if(bmip_client_events_counter >= BMIP_EV_TRACE_LIMIT)
                        bmip_client_events_counter = 0;
                int eindex = bmip_client_events_counter++;
                bmip_client_events[eindex] = etype;
                bmip_client_pid[eindex] = ev.initiator.pid;
                bmip_client_nid[eindex] = ev.initiator.nid;
                bmip_client_mb[eindex] = ev.match_bits;
                bmip_client_handler[eindex] = 4;
#endif
                if(etype == PTL_EVENT_SEND_END)
                {
                        ecount++;
                }
                else if(etype == PTL_EVENT_ACK)
                {
                        ecount++;
                }
                else
                {
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)bmip_safe_malloc(sizeof(bmip_pending_event_t), __FILE__, __func__, __LINE__);
                        pe->eventid = etype;
                        pe->event = ev;
			qlist_add_tail(&pe->list, pelist);
                }
        }

	return 0;
}

int bmip_wait_remote_put(struct qlist_head * pelist)
{
	int ecount = 0;
	int etype = 0;
        int i = 0;

        bmip_pending_event_t * n = NULL, * nsafe = NULL;
        qlist_for_each_entry_safe(n, nsafe, pelist, list)
        {
                if(n->eventid == PTL_EVENT_PUT_END)
                {
                        qlist_del(&n->list);
			bmip_safe_free(n, __FILE__, __func__, __LINE__);
                        ecount++;
                }
                if(ecount == 1)
                        break;
        }

        while(ecount < 1)
        {
		ptl_event_t ev;
                etype = bmip_wait_ex_event(&ev);

#ifdef BMIP_TRACE
                /* track all of the events */
                if(bmip_client_events_counter >= BMIP_EV_TRACE_LIMIT)
                        bmip_client_events_counter = 0;
                int eindex = bmip_client_events_counter++;
                bmip_client_events[eindex] = etype;
                bmip_client_pid[eindex] = ev.initiator.pid;
                bmip_client_nid[eindex] = ev.initiator.nid;
                bmip_client_mb[eindex] = ev.match_bits;
                bmip_client_handler[eindex] = 5;
#endif
                if(etype == PTL_EVENT_PUT_END)
                {
                        ecount++;
                }
                else
                {
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)bmip_safe_malloc(sizeof(bmip_pending_event_t), __FILE__, __func__, __LINE__);
                        pe->eventid = etype;
                        pe->event = ev;
			qlist_add_tail(&pe->list, pelist);
                }
        }
	return 0;
}

int bmip_client_unex_send(ptl_process_id_t target, int num, void * buffer, size_t length, int tag)
{
        /* generic variables */
        int ecount = 0;
        int ret = PTL_OK;
        int i = 0;
	bmip_pending_event_t * n = NULL, * nsafe = NULL;
	QLIST_HEAD(pelist);
        int tagseq = (tag << 3);

        /* trace ids */
        uint64_t csid = 0;
        uint64_t ptlid = 0;

        /* trace enter */
        csid = bmip_trace_get_index();
        bmip_trace_add(target, BMIP_CLIENT_UNEX_SEND_START, tag, csid);

        bmip_client_seq++;
	
        /* handles and mdesc for each request */
        ptl_handle_md_t put_req_md = PTL_HANDLE_INVALID;
        ptl_md_t put_req_mdesc;

        /* match bits */
        ptl_match_bits_t mb = tagseq;

        /* setup of the MDs */
        put_req_mdesc.start = buffer;
        put_req_mdesc.length = 0;
        put_req_mdesc.threshold = 2; /* send, ack */
        put_req_mdesc.options = PTL_MD_OP_PUT;
        put_req_mdesc.eq_handle = bmi_eq;

        ret = bmip_ptl_md_bind(local_ni, put_req_mdesc, PTL_RETAIN, &put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                goto out;
        }

	/* copy the unex message into the unex msg space */
	memcpy(bmip_unex_space, buffer, length);

        /* trace enter */
        ptlid = bmip_trace_get_index();
        bmip_trace_add(target, PTL_LOCAL_PUT_START, tag, ptlid);

        /* 1a: send put request */
        ret = bmip_ptl_put(put_req_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | unex_mb, 0, length);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }

	bmip_wait_local_put(&pelist);

	/* trace exit */
        bmip_trace_add(target, PTL_LOCAL_PUT_END, tag, ptlid);

	/* unlink the request */
	ret = bmip_ptl_md_unlink(put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req md", __FILE__, __func__, __LINE__);
                goto out;
        }	
	put_req_md = PTL_HANDLE_INVALID;

        /* trace enter */
        ptlid = bmip_trace_get_index();
        bmip_trace_add(target, PTL_REMOTE_GET_START, tag, ptlid);

	bmip_wait_unex_remote_get(&pelist);

	/* trace exit */
        bmip_trace_add(target, PTL_REMOTE_GET_END, tag, ptlid);

out:
        qlist_for_each_entry_safe(n, nsafe, &pelist, list)
        {
		qlist_del(&n->list);
		bmip_safe_free(n, __FILE__, __func__, __LINE__);
	}

	/* trace exit */
        bmip_trace_add(target, BMIP_CLIENT_UNEX_SEND_END, tag, csid);

	return 1;
}

int bmip_client_send(ptl_process_id_t target, int num, void ** buffers, size_t * lengths, int tag)
{
	/* generic variables */
	size_t offset = 0;
	int ecount = 0;
	int ret = PTL_OK;
	int i = 0;
	bmip_pending_event_t * n = NULL, * nsafe = NULL;
	QLIST_HEAD(pelist);
	int tagseq = (tag << 3);

	/* trace ids */
	uint64_t csid = 0;
	uint64_t ptlid = 0;

	/* trace enter */
	csid = bmip_trace_get_index();
	bmip_trace_add(target, BMIP_CLIENT_EX_SEND_START, tag, csid);

	bmip_client_seq++;

	/* handles and mdesc for each request */
	ptl_handle_md_t put_req_md = PTL_HANDLE_INVALID;
	ptl_handle_md_t put_md = PTL_HANDLE_INVALID;
        ptl_md_t put_req_mdesc;
        ptl_md_t put_mdesc;

	/* match bits */
        ptl_match_bits_t mb = tagseq;

	/* setup of the MDs */
        put_req_mdesc.start = buffers[0];
        put_req_mdesc.length = 0;
        put_req_mdesc.threshold = 2; /* send, ack */
        put_req_mdesc.options = PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	put_req_mdesc.eq_handle = bmi_eq;

        ret = bmip_ptl_md_bind(local_ni, put_req_mdesc, PTL_RETAIN, &put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                goto out;
        }

        put_mdesc.threshold = 2; /* send, ack */
        put_mdesc.options = PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	put_mdesc.eq_handle = bmi_eq;

	/* setup the request meta info */
	bmip_ex_req_space[0] = num;
	for(i = 0 ; i < num ; i++)
	{
		/* set the msg length */
		bmip_ex_req_space[i + 1] = lengths[i];
	}

	/* trace enter */
	ptlid = bmip_trace_get_index();
	bmip_trace_add(target, PTL_LOCAL_PUT_START, tag, ptlid);

	/* send put request */
        ret = bmip_ptl_put(put_req_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_req_mb | ex_req_put_mb, 0, num);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }


	bmip_wait_local_put(&pelist);

	/* trace exit */
	bmip_trace_add(target, PTL_LOCAL_PUT_END, tag, ptlid);

	/* trace enter */
	ptlid = bmip_trace_get_index();
	bmip_trace_add(target, PTL_REMOTE_GET_START, tag, ptlid);

	bmip_wait_remote_get(&pelist);

	/* trace exit */
	bmip_trace_add(target, PTL_REMOTE_GET_END, tag, ptlid);

	/* trace enter */
	ptlid = bmip_trace_get_index();
	bmip_trace_add(target, PTL_REMOTE_GET_START, tag, ptlid);

	bmip_wait_remote_put(&pelist);
	
	/* trace exit */
	bmip_trace_add(target, PTL_REMOTE_GET_END, tag, ptlid);

	/* cleanup */
	ret = bmip_ptl_md_unlink(put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req md", __FILE__, __func__, __LINE__);
                goto out;
        }	
	put_req_md = PTL_HANDLE_INVALID;
	
	int snum = bmip_ex_rsp_space[0];

	if(num == snum)
	{
		/* set the remote offset */
		for(i = 0 ; i < num ; i++)
		{
        		put_mdesc.start = buffers[i];
        		put_mdesc.length = lengths[i];
        	
			ret = bmip_ptl_md_bind(local_ni, put_mdesc, PTL_RETAIN, &put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                		goto out;
	        	}

			offset = bmip_ex_rsp_space[i + 1];

			/* trace enter */
			ptlid = bmip_trace_get_index();
			bmip_trace_add(target, PTL_LOCAL_PUT_START, tag, ptlid);

			/* invoke the put */
	        	ret = bmip_ptl_put(put_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset, 0);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
		
			/* wait for ack */
			bmip_wait_local_put(&pelist);

			/* trace exit */
			bmip_trace_add(target, PTL_LOCAL_PUT_END, tag, ptlid);

			/* cleanup */
			ret = bmip_ptl_md_unlink(put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
                		goto out;
	        	}	
			put_md = PTL_HANDLE_INVALID;
		}
	}
	else if(snum > num)
	{
		void * cur_buffer = buffers[0];

		/* set the remote offset */
		for(i = 0 ; i < snum ; i++)
		{
			size_t rlength = bmip_ex_rsp_space[snum + i + 1];
        		put_mdesc.start = cur_buffer;
        		put_mdesc.length = rlength;
        	
			ret = bmip_ptl_md_bind(local_ni, put_mdesc, PTL_RETAIN, &put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                		goto out;
	        	}

			offset = bmip_ex_rsp_space[i + 1];

			/* trace enter */
			ptlid = bmip_trace_get_index();
			bmip_trace_add(target, PTL_LOCAL_PUT_START, tag, ptlid);

			/* invoke the put */
	        	ret = bmip_ptl_put(put_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset, 0);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
		
			/* wait for ack */
			bmip_wait_local_put(&pelist);

			/* trace exit */
			bmip_trace_add(target, PTL_LOCAL_PUT_END, tag, ptlid);

			/* cleanup */
			ret = bmip_ptl_md_unlink(put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
                		goto out;
	        	}	
			put_md = PTL_HANDLE_INVALID;

			cur_buffer += rlength;
		}
	}
	else
	{
		offset = bmip_ex_rsp_space[1];

		/* set the remote offset */
		for(i = 0 ; i < num ; i++)
		{
        		put_mdesc.start = buffers[i];
        		put_mdesc.length = lengths[i];
        	
			ret = bmip_ptl_md_bind(local_ni, put_mdesc, PTL_RETAIN, &put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                		goto out;
	        	}

			/* trace enter */
			ptlid = bmip_trace_get_index();
			bmip_trace_add(target, PTL_LOCAL_PUT_START, tag, ptlid);

			/* invoke the put */
	        	ret = bmip_ptl_put(put_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset, 0);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
		
			/* wait for ack */
			bmip_wait_local_put(&pelist);

			/* trace exit */
			bmip_trace_add(target, PTL_LOCAL_PUT_END, tag, ptlid);

			/* cleanup */
			ret = bmip_ptl_md_unlink(put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
                		goto out;
	        	}	
			put_md = PTL_HANDLE_INVALID;

			offset += lengths[i];
		}
	}

out:
        qlist_for_each_entry_safe(n, nsafe, &pelist, list)
        {
                qlist_del(&n->list);
		bmip_safe_free(n, __FILE__, __func__, __LINE__);
        }
	
	/* exit trace */
	bmip_trace_add(target, BMIP_CLIENT_EX_SEND_END, tag, csid);

	return ret;
}

int bmip_client_recv(ptl_process_id_t target, int num, void ** buffers, size_t * lengths, int tag)
{
	/* generic variables */
	size_t offset = 0;
	int ecount = 0;
	int ret = PTL_OK;
	int i = 0;
	bmip_pending_event_t * n = NULL, * nsafe = NULL;
	QLIST_HEAD(pelist);
        int tagseq = (tag << 3);

        /* trace ids */
        uint64_t csid = 0;
        uint64_t ptlid = 0;

	/* trace enter */
	csid = bmip_trace_get_index();
	bmip_trace_add(target, BMIP_CLIENT_EX_RECV_START, tag, csid);

        bmip_client_seq++;

	/* handles and mdesc for each request */
	ptl_handle_md_t put_req_md = PTL_HANDLE_INVALID;
	ptl_handle_md_t put_md = PTL_HANDLE_INVALID;
        ptl_md_t put_req_mdesc;
        ptl_md_t put_mdesc;

	/* match bits */
        ptl_match_bits_t mb = tagseq;

	/* setup of the MDs */
        put_req_mdesc.start = buffers[0];
        put_req_mdesc.length = 0;
        put_req_mdesc.threshold = 2; /* send, ack */
        put_req_mdesc.options = PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	put_req_mdesc.eq_handle = bmi_eq;

        ret = bmip_ptl_md_bind(local_ni, put_req_mdesc, PTL_RETAIN, &put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
		exit(0);
                goto out;
        }

        put_mdesc.threshold = 2; /* send, ack */
        put_mdesc.options = PTL_MD_OP_GET | PTL_MD_MANAGE_REMOTE;
	put_mdesc.eq_handle = bmi_eq;

	/* set the msg length */
	bmip_ex_req_space[0] = num;
	for(i = 0 ; i < num ; i++)
	{
		bmip_ex_req_space[i + 1] = lengths[i];
	}

	/* trace enter */
	ptlid = bmip_trace_get_index();
	bmip_trace_add(target, PTL_LOCAL_PUT_START, tag, ptlid);

	/* send put request */
        ret = bmip_ptl_put(put_req_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_req_mb | ex_req_get_mb, 0, num);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }

	/* wait for ack... 4 events */
	bmip_wait_local_put(&pelist);
	
	/* trace exit */
	bmip_trace_add(target, PTL_LOCAL_PUT_END, tag, ptlid);

	/* cleanup */
	ret = bmip_ptl_md_unlink(put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req md", __FILE__, __func__, __LINE__);
                goto out;
        }	
	put_req_md = PTL_HANDLE_INVALID;

	/* trace enter */
	ptlid = bmip_trace_get_index();
	bmip_trace_add(target, PTL_REMOTE_PUT_START, tag, ptlid);

	bmip_wait_remote_put(&pelist);

	/* trace exit */
	bmip_trace_add(target, PTL_REMOTE_PUT_END, tag, ptlid);

	int snum = bmip_ex_rsp_space[0];

	if(snum == num)
	{
		/* set the remote offset */
		for(i = 0 ; i < num ; i++)
		{
			size_t rlength = 0;

			/* next offset and length */
			offset = bmip_ex_rsp_space[i + 1];
			rlength = bmip_ex_rsp_space[num + i + 1];

        		put_mdesc.start = buffers[i];

			/* get the min amount of data */
			if(rlength < lengths[i])
			{
        			put_mdesc.length = rlength;
				lengths[i] = rlength;
			}
			else
			{
        			put_mdesc.length = lengths[i];
			}

        		ret = bmip_ptl_md_bind(local_ni, put_mdesc, PTL_RETAIN, &put_md);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                		goto out;
        		}

			/* trace enter */
			ptlid = bmip_trace_get_index();
			bmip_trace_add(target, PTL_LOCAL_GET_START, tag, ptlid);

			/* invoke the put */
	        	ret = bmip_ptl_get(put_md, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not recv ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
	
			/* wait for ack */
			bmip_wait_local_get(&pelist);
	
			/* trace exit */
			bmip_trace_add(target, PTL_LOCAL_GET_END, tag, ptlid);

			/* cleanup */
			ret = bmip_ptl_md_unlink(put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not unlink recv msg md", __FILE__, __func__, __LINE__);
                		goto out;
	        	}	
			put_md = PTL_HANDLE_INVALID;
		}
	}
	else if(snum > num)
	{
		void * cur_buffer = buffers[0];

		/* set the remote offset */
		for(i = 0 ; i < snum ; i++)
		{
			unsigned int rlength = 0;

			/* next offset and length */
			offset = bmip_ex_rsp_space[i + 1];
			rlength = bmip_ex_rsp_space[snum + i + 1];

        		put_mdesc.start = cur_buffer;

			/* get the min amount of data */
			if(rlength < lengths[0])
			{
				if(i == 0)
					lengths[0] = rlength;
				else
					lengths[0] += rlength;
			}
			else
			{
				rlength = lengths[0];
			}
        		put_mdesc.length = rlength;

        		ret = bmip_ptl_md_bind(local_ni, put_mdesc, PTL_RETAIN, &put_md);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                		goto out;
        		}

			/* trace enter */
			ptlid = bmip_trace_get_index();
			bmip_trace_add(target, PTL_LOCAL_GET_START, tag, ptlid);

			/* invoke the put */
	        	ret = bmip_ptl_get(put_md, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not recv ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
	
			/* wait for ack */
			bmip_wait_local_get(&pelist);
	
			/* trace exit */
			bmip_trace_add(target, PTL_LOCAL_GET_END, tag, ptlid);

			/* cleanup */
			ret = bmip_ptl_md_unlink(put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not unlink recv msg md", __FILE__, __func__, __LINE__);
                		goto out;
	        	}	
			put_md = PTL_HANDLE_INVALID;

			/* update the cur location into the local buffer */
			cur_buffer += rlength;
		}
	}
	else
	{
		offset = bmip_ex_rsp_space[1];
		unsigned int rlength = bmip_ex_rsp_space[snum + 1];

		/* set the remote offset */
		for(i = 0 ; i < num ; i++)
		{
        		put_mdesc.start = buffers[i];

			/* get the min amount of data */
			if(rlength < lengths[i])
			{
        			put_mdesc.length = rlength;
				lengths[i] = rlength;
			}
			else
			{
        			put_mdesc.length = lengths[i];
			}

        		ret = bmip_ptl_md_bind(local_ni, put_mdesc, PTL_RETAIN, &put_md);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                		goto out;
        		}

			/* trace enter */
			ptlid = bmip_trace_get_index();
			bmip_trace_add(target, PTL_LOCAL_GET_START, tag, ptlid);

			/* invoke the put */
	        	ret = bmip_ptl_get(put_md, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not recv ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
	
			/* wait for ack */
			bmip_wait_local_get(&pelist);
	
			/* trace exit */
			bmip_trace_add(target, PTL_LOCAL_GET_END, tag, ptlid);

			/* cleanup */
			ret = bmip_ptl_md_unlink(put_md);
	        	if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not unlink recv msg md", __FILE__, __func__, __LINE__);
                		goto out;
	        	}	
			put_md = PTL_HANDLE_INVALID;

			/* update the remote offset */
			offset += lengths[i];
		}
	}

out:
        qlist_for_each_entry_safe(n, nsafe, &pelist, list)
        {
                qlist_del(&n->list);
		bmip_safe_free(n, __FILE__, __func__, __LINE__);
        }

	/* exit trace */
	bmip_trace_add(target, BMIP_CLIENT_EX_RECV_END, tag, csid);

	return ret;
}

int bmip_server_post_unex_send(ptl_process_id_t target, int num, void * buffer, size_t length, int tag, void * user_ptr, int64_t comm_id)
{
	/* if this is a message to self */
	if(target.pid == local_pid.pid && target.nid == local_pid.nid)
	{
		char c='u';
		int ret = 0;
		bmip_portals_conn_op_t * r_cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
        	memset(r_cur_op, 0, sizeof(bmip_portals_conn_op_t));
		bmip_portals_conn_op_t * s_cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
        	memset(s_cur_op, 0, sizeof(bmip_portals_conn_op_t));

		bmip_get_addr_seq(target, 1);

		/* setup the recv op */
        	r_cur_op->match_bits = tag;
		r_cur_op->unex_msg = bmip_safe_malloc(length, __FILE__, __func__, __LINE__);
		r_cur_op->unex_msg_len = length;
		r_cur_op->target = target;
		memcpy(r_cur_op->unex_msg, buffer, length);

		/* setup send op */
		s_cur_op->target = target;
		s_cur_op->num = num;
		s_cur_op->comm_id = comm_id;
		s_cur_op->user_ptr = user_ptr;
        	s_cur_op->match_bits = tag;

		gen_mutex_lock(&list_mutex);
		qlist_add_tail(&r_cur_op->list, &bmip_unex_done);
		qlist_add_tail(&s_cur_op->list, &bmip_done_ops);

		/* notify */
		ret = write(unexworkfds[1], &c, 1);
		if(ret != 1)
			fprintf(stderr, "%s:%i error... could not write to unex pipe\n", __func__, __LINE__);
        
		ret = write(workfds[1], &c, 1);
		if(ret != 1)
			fprintf(stderr, "%s:%i error... could not write to ex pipe\n", __func__, __LINE__);
        
		/* update the count */
		server_num_unex_done++;
		server_num_done++;
		gen_mutex_unlock(&list_mutex);
		return 0;
	}
	
        /* generic variables */
        int ecount = 0;
        int ret = PTL_OK;
        int i = 0;
        int tagseq = (tag << 3);

        /* trace ids */
        uint64_t csid = 0;
        uint64_t ptlid = 0;

	/* create op */
        bmip_portals_conn_op_t * cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
        memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

        /* setup op info */
        cur_op->target = target;
	cur_op->buffers = (void **)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
	cur_op->lengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);

	cur_op->unex_buffer = (void *)bmip_safe_malloc(sizeof(char) * length, __FILE__, __func__, __LINE__);
	memcpy(cur_op->unex_buffer, buffer, length);
	cur_op->unex_buffer_length = length;
	
	for(i = 0 ; i < num ; i++)
	{
		cur_op->buffers[i] = buffer;
		cur_op->lengths[i] = length;
	}
	cur_op->num = num;
	cur_op->comm_id = comm_id;
	cur_op->user_ptr = user_ptr;
        cur_op->match_bits = 0xffffffffULL & tagseq;
        cur_op->op_type = BMIP_UNEX_SEND;
	cur_op->unexmb = tagseq;

        bmip_client_seq++;
	
        /* match bits */
        ptl_match_bits_t mb = tagseq;

	gen_mutex_lock(&list_mutex);
	if(bmip_s2s_unexsend_active != 0)
	{
		cur_op->cur_function = bmip_server_unex_start_op;
		qlist_add_tail(&cur_op->list, &bmip_s2s_unexsend);
	}
	else
	{
		bmip_s2s_unexsend_active = 1;
		bmip_server_unex_start_op(cur_op, BMIP_UNEX_SS_LOCK_HELD);
	}
	gen_mutex_unlock(&list_mutex);
out:
	return 0;
}

int bmip_server_unex_start_op(void * op_, int etype)
{
	int ret = 0;
	bmip_portals_conn_op_t * cur_op = (bmip_portals_conn_op_t *)op_;

       /* setup of the MDs */
        cur_op->mdesc.start = cur_op->unex_buffer;
        cur_op->mdesc.length = 0;
        cur_op->mdesc.threshold = 2; /* send, ack */
        cur_op->mdesc.options = PTL_MD_OP_PUT;
        cur_op->mdesc.eq_handle = bmi_eq;

        ret = bmip_ptl_md_bind(local_ni, cur_op->mdesc, PTL_RETAIN, &cur_op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                goto out;
        }

	/* copy the unex message into the unex msg space */
	memcpy(bmip_unex_space, cur_op->unex_buffer, cur_op->unex_buffer_length);

        /* send put request */
        ret = bmip_ptl_put(cur_op->md, PTL_ACK_REQ, cur_op->target, BMIP_PTL_INDEX, 0, cur_op->unexmb | unex_mb | unex_server_traffic_mb, 0, cur_op->unex_buffer_length);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }

	/* goto the server unex send wait_local_put state */
#ifdef BMIP_COUNT_ALL_EVENTS 
        cur_op->put_push_rsp_wait_counter = 3;
#else
        cur_op->put_push_rsp_wait_counter = 1;
#endif
        cur_op->cur_function = bmip_server_unex_send_put_local_put_wait;

	if(etype == BMIP_UNEX_SS_LOCK_FREE)
	{
		gen_mutex_lock(&list_mutex);
		qlist_add_tail(&cur_op->list, &bmip_unex_ss_pending);
		gen_mutex_unlock(&list_mutex);
	}
	else if(etype == BMIP_UNEX_SS_LOCK_HELD)
	{
		qlist_add_tail(&cur_op->list, &bmip_unex_ss_pending);
	}
out:
	return 0;
}

int bmip_server_unex_send_put_remote_put(bmip_portals_conn_op_t * cur_op, int etype)
{
	/* unlink the request */
	int ret = bmip_ptl_md_unlink(cur_op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req md", __FILE__, __func__, __LINE__);
                goto out;
        }	
	cur_op->md = PTL_HANDLE_INVALID;

        cur_op->get_remote_get_wait_counter = 1;
        cur_op->cur_function = bmip_server_unex_send_get_remote_get_wait;

out:
	return 0;
}

int bmip_server_unex_send_cleanup(void * op_, int etype)
{
	int ret = 0;
	char c = 'd';
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	op->match_bits = (op->match_bits >> 3);

	/* free the local unex buffer */
	if(op->unex_buffer)
		bmip_safe_free(op->unex_buffer, __FILE__, __func__, __LINE__);	

        /* remove from the list */
        qlist_del(&op->list);
        qlist_add_tail(&op->list, &bmip_done_ops);

	/* write to the done pipe */
        ret = write(workfds[1], &c, 1);
        if(ret != 1)
	        fprintf(stderr, "%s:%i error... could not write to ex pipe\n", __func__, __LINE__);

	/* inc the num unex msg count... this case for a send */
        server_num_done++;

	op = bmip_ss_unex_pending();
	if(op != NULL)
	{
		op->cur_function(op, BMIP_UNEX_SS_LOCK_HELD);
	}
	else
	{
		/* set to not active */
		bmip_s2s_unexsend_active = 0;
	}
	return 0;
}

int bmip_server_put_local_get_req_info(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	/* setup the callback and params */
#ifdef BMIP_COUNT_ALL_EVENTS 
	op->put_fetch_req_wait_counter = 4;
#else
	op->put_fetch_req_wait_counter = 2;
#endif
	op->cur_function = bmip_server_put_local_get_req_wait;

	return 0;
}

int bmip_server_put_local_get_req_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_SEND_START)
        {
		op->put_fetch_req_wait_counter--;
        }
        else if(etype == PTL_EVENT_SEND_END)
        {
		op->put_fetch_req_wait_counter--;
        }
        else if(etype == PTL_EVENT_REPLY_START)
#else
        if(etype == PTL_EVENT_REPLY_START)
#endif
        {
		op->put_fetch_req_wait_counter--;
        }
        else if(etype == PTL_EVENT_REPLY_END)
        {
		op->put_fetch_req_wait_counter--;
        }

	/* if counter done... invoke the next function */
	if(op->put_fetch_req_wait_counter == 0)
	{
        	bmip_trace_add(op->target, PTL_LOCAL_GET_END, op->match_bits >> 3, op->ptlid);
		bmip_server_put_local_put_rsp_info(op, etype);
	}

	return 0;
}

int bmip_server_put_local_put_rsp_info(void * op_, int etype)
{
	int ret = 0;
	int i = 0;
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

        /* cleanup */
        ret = bmip_ptl_md_unlink(op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req ex msg", __FILE__, __func__, __LINE__);
        }

	op->req_buffer[0] = op->num;
	for(i = 1 ; i < op->num + 1 ; i++)
	{
		op->req_buffer[i] = op->offsets[i - 1];
		op->req_buffer[i + op->num] = op->lengths[i - 1];
	}

 	/* setup of the MDs */
	op->mdesc.start = op->req_buffer;
       	op->mdesc.length = sizeof(size_t) * (2 * op->num + 1);
       	op->mdesc.threshold = 2; /* send, ack */
       	op->mdesc.options = PTL_MD_OP_PUT;
       	op->mdesc.eq_handle = bmi_eq;

       	ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
       	if(ret != PTL_OK)
       	{
       		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
       	}

	op->ptlid = bmip_trace_get_index();
        bmip_trace_add(op->target, PTL_LOCAL_PUT_START, op->match_bits >> 3, op->ptlid);

	/* send back the offset to use */
	ret = bmip_ptl_put(op->md, PTL_ACK_REQ, op->target, BMIP_PTL_INDEX, 0, (op->match_bits & 0xffffffffULL) | ex_rsp_mb, 0, 0);
       	if(ret != PTL_OK)
       	{
       		bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
       	}

	/* setup the callback and params */
#ifdef BMIP_COUNT_ALL_EVENTS 
	op->put_push_rsp_wait_counter = 3;
#else
	op->put_push_rsp_wait_counter = 1;
#endif
	op->cur_function = bmip_server_put_local_put_wait;

	return 0;
}

int bmip_server_unex_send_put_local_put_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_SEND_START)
        {
		op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_SEND_END)
        {
		op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_ACK)
#else
        if(etype == PTL_EVENT_ACK)
#endif
        {
		op->put_push_rsp_wait_counter--;
        }

	/* if counter done... set next function */
	if(op->put_push_rsp_wait_counter == 0)
	{
		bmip_server_unex_send_put_remote_put(op, etype);
	}

	return 0;
}

int bmip_server_to_server_transfer_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_SEND_START)
        {
		op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_SEND_END)
        {
		op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_ACK)
#else
        if(etype == PTL_EVENT_ACK)
#endif
        {
		op->put_push_rsp_wait_counter--;
        }

	/* if counter done... set next function */
	if(op->put_push_rsp_wait_counter == 0)
	{
		bmip_server_to_server_send_5_cleanup(op, etype);
	}

	return 0;
}

int bmip_server_put_local_put_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_SEND_START)
        {
		op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_SEND_END)
        {
		op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_ACK)
#else
        if(etype == PTL_EVENT_ACK)
#endif
        {
		op->put_push_rsp_wait_counter--;
        }

	/* if counter done... set next function */
	if(op->put_push_rsp_wait_counter == 0)
	{
        	bmip_trace_add(op->target, PTL_LOCAL_PUT_END, op->match_bits >> 3, op->ptlid);
		bmip_server_put_remote_put(op, etype);
	}

	return 0;
}

int bmip_server_put_remote_put(void * op_, int etype)
{
	int ret = 0;
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

        /* cleanup */
        ret = bmip_ptl_md_unlink(op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req ex msg", __FILE__, __func__, __LINE__);
        }

	/* setup the callback */
	int nm = 1;

	op->ptlid = bmip_trace_get_index();
       	bmip_trace_add(op->target, PTL_REMOTE_PUT_START, op->match_bits >> 3, op->ptlid);
	if(op->num >= op->rnum)
	{
		op->put_remote_put_wait_counter = nm * (op->num);
	}
	else
	{
		op->put_remote_put_wait_counter = nm * (op->rnum);
	}
	op->cur_function = bmip_server_put_remote_put_wait;

	return 0;
}

int bmip_server_put_remote_put_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
        if(etype == PTL_EVENT_PUT_END)
        {
		op->put_remote_put_wait_counter--;
        }

	if(op->put_remote_put_wait_counter == 0)
	{
        	bmip_trace_add(op->target, PTL_REMOTE_PUT_END, op->match_bits >> 3, op->ptlid);
		bmip_server_put_cleanup(op, etype);
	}
	return 0;
}



int bmip_server_put_cleanup(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	op->match_bits = (op->match_bits >> 3);

	/* remove from the list */
	qlist_del(&op->list);

	/* release the context */

	/* if we use a barrier, signal the barrier */
	if(op->use_barrier)
	{
		pthread_barrier_wait(&op->context->b);
		pthread_barrier_destroy(&op->context->b);
		bmip_safe_free(op->context, __FILE__, __func__, __LINE__);	
	
		/* free the op memory */
		bmip_safe_free(op->lengths, __FILE__, __func__, __LINE__);
		bmip_safe_free(op, __FILE__, __func__, __LINE__);
	}
	/* else, add to the done list and hit the cv */
	else
	{
		int i = 0;
		char c = 'p';
		int ret = 0;

		for(i = 0 ; i < op->num ; i++)
		{
			if(op->user_buffers[i] != op->buffers[i])
			{
				bmip_new_free(op->user_buffers[i]);
			}
		}

		if(op->num > 0 && op->user_buffers)
		{
			bmip_safe_free(op->user_buffers, __FILE__, __func__, __LINE__);
		}

		qlist_add_tail(&op->list, &bmip_done_ops);
		ret = write(workfds[1], &c, 1);
		if(ret != 1)
			fprintf(stderr, "%s:%i error... could not write to put ex pipe\n", __func__, __LINE__);
		server_num_done++;
                bmip_trace_add(op->target, BMIP_SERVER_EX_RECV_DRIVER_END, op->match_bits >> 3, op->csid);	
	}

	return 0;
}

int bmip_server_get_local_put_rsp_info(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_SEND_START)
        {
		op->get_local_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_SEND_END)
        {
		op->get_local_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_ACK)
#else
        if(etype == PTL_EVENT_ACK)
#endif
        {
		op->get_local_rsp_wait_counter--;
        }

	/* invoke */
	if(op->get_local_rsp_wait_counter == 0)
	{
        	bmip_trace_add(op->target, PTL_LOCAL_PUT_END, op->match_bits >> 3, op->ptlid);
		bmip_server_get_remote_get(op, etype);
	}

	return 0;
}

int bmip_server_get_remote_get(void * op_, int etype)
{
	int ret = 0;
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

        /* cleanup */
        ret = bmip_ptl_md_unlink(op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req ex msg", __FILE__, __func__, __LINE__);
        }

	/* setup the callback */
	int nm = 1;

	op->ptlid = bmip_trace_get_index();
       	bmip_trace_add(op->target, PTL_REMOTE_GET_START, op->match_bits >> 3, op->ptlid);
	if(op->num >= op->rnum)
	{
		op->get_remote_get_wait_counter = nm * op->num;
	}
	else
	{
		op->get_remote_get_wait_counter = nm * op->rnum;
	}
	op->cur_function = bmip_server_get_remote_get_wait;

	return 0;
}

int bmip_server_unex_send_get_remote_get_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
        if(etype == PTL_EVENT_GET_END)
        {
                op->get_remote_get_wait_counter--;
        }

	if(op->get_remote_get_wait_counter == 0)
	{
		bmip_server_unex_send_cleanup(op, etype);
	}
	return 0;
}

int bmip_server_get_remote_get_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
        if(etype == PTL_EVENT_GET_END)
        {
                op->get_remote_get_wait_counter--;
        }

	if(op->get_remote_get_wait_counter == 0)
	{
        	bmip_trace_add(op->target, PTL_REMOTE_GET_END, op->match_bits >> 3, op->ptlid);
		bmip_server_get_cleanup(op, etype);
	}
	return 0;
}

int bmip_server_get_cleanup(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	op->match_bits = (op->match_bits >> 3);

	/* remove from the list */
	qlist_del(&op->list);

	/* release the context */
	if(op->use_barrier)
	{
		pthread_barrier_wait(&op->context->b);
		pthread_barrier_destroy(&op->context->b);
		bmip_safe_free(op->context, __FILE__, __func__, __LINE__);
	
		/* free the op memory */
		bmip_safe_free(op->lengths, __FILE__, __func__, __LINE__);
		bmip_safe_free(op, __FILE__, __func__, __LINE__);
	}
	else
	{
		/* copy the recv'd buffers to the user buffers */
		int i = 0;
		char c = 'g';
		int ret = 0;

		for(i = 0 ; i < op->num ; i++)
		{
			if(op->user_buffers[i] != op->buffers[i])
			{
				memcpy(op->user_buffers[i], op->buffers[i], op->lengths[i]);
				bmip_new_free(op->user_buffers[i]);
			}
		}

		if(op->num > 0 && op->user_buffers)
		{
			bmip_safe_free(op->user_buffers, __FILE__, __func__, __LINE__);
		}

		qlist_add_tail(&op->list, &bmip_done_ops);
		ret = write(workfds[1], &c, 1);
		if(ret != 1)
			fprintf(stderr, "%s:%i error... could not write to get ex pipe\n", __func__, __LINE__);
		server_num_done++;
                bmip_trace_add(op->target, BMIP_SERVER_EX_SEND_DRIVER_END, op->match_bits >> 3, op->csid);	
	}

	return 0;
}

int bmip_server_put_pending(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
	op->cur_function = NULL;
	return 0;
}

int bmip_server_get_pending(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
	op->cur_function = NULL;
	return 0;
}

int bmip_server_put_init(void * op_, int etype, ptl_process_id_t pid)
{
	int ret = 0;
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

        /* setup of the MDs */
        op->mdesc.start = op->req_buffer;
        op->mdesc.length = 2 * sizeof(size_t);
        op->mdesc.threshold = 2; /* send, ack */
        op->mdesc.options = PTL_MD_OP_GET;
        op->mdesc.eq_handle = bmi_eq;
	op->target = pid;

        ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
        }

	op->ptlid = bmip_trace_get_index();
        bmip_trace_add(op->target, PTL_LOCAL_GET_START, op->match_bits >> 3, op->ptlid);

        ret = bmip_ptl_get(op->md, op->target, BMIP_PTL_INDEX, 0, (op->match_bits & 0xffffffffULL) | ex_req_mb, 0);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
        }

        /* set the callback */
        bmip_server_put_local_get_req_info(op, etype);

        /* add the op the queue */
        qlist_add_tail(&op->list, &bmip_cur_ops);

	return 0;
}

int bmip_server_get_init(void * op_, int etype, ptl_process_id_t op_pid)
{	
	int ret = 0;
	int i = 0;

	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	op->req_buffer[0] = op->num;
	for(i = 1 ; i < op->num + 1 ; i++)
	{
		op->req_buffer[i] = op->offsets[i - 1];
		op->req_buffer[i + op->num] = op->lengths[i - 1];
	}

	/* setup of the MDs */
	op->mdesc.start = op->req_buffer;
	op->mdesc.length = sizeof(size_t) * (2 * op->num + 1);
	op->mdesc.threshold = 2; /* send, ack */
	op->mdesc.options = PTL_MD_OP_PUT;
	op->mdesc.eq_handle = bmi_eq;
	op->target = op_pid;

	ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
	}

	op->ptlid = bmip_trace_get_index();
        bmip_trace_add(op->target, PTL_LOCAL_PUT_START, op->match_bits >> 3, op->ptlid);

	/* send back the offset to use */
	ret = bmip_ptl_put(op->md, PTL_ACK_REQ, op->target, BMIP_PTL_INDEX, 0, (op->match_bits & 0xffffffffULL) | ex_rsp_mb, 0, 0);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
	}

	op->cur_function = bmip_server_get_local_put_rsp_info;
#ifdef BMIP_COUNT_ALL_EVENTS 
	op->get_local_rsp_wait_counter = 3;
#else
	op->get_local_rsp_wait_counter = 1;
#endif

        /* add the op the queue */
        qlist_add_tail(&op->list, &bmip_cur_ops);

	return 0;
}

int bmip_server_post_recv(ptl_process_id_t target, int64_t match_bits, int num, void ** buffers, size_t * lengths, int use_barrier, void * user_ptr, int64_t comm_id)
{
	int ret = 0;
	bmip_portals_conn_op_t * cur_op = NULL;
	int i = 0;
	unsigned int s = 0;
	int64_t omb = match_bits; 

	gen_mutex_lock(&list_mutex);

        /* update the sequence and match bits */
        s = bmip_get_addr_seq(target, 2);
        match_bits = (match_bits << 3) | (s & 0x7);

	if(local_pid.pid == target.pid && local_pid.nid == target.nid)
	{
                int ret = 0;
                bmip_portals_conn_op_t * p_send_op = NULL;
                bmip_portals_conn_op_t * r_cur_op = NULL;

		r_cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
		memset(r_cur_op, 0, sizeof(bmip_portals_conn_op_t));

		/* setup op info */
		r_cur_op->target = target;
		r_cur_op->match_bits = 0xffffffffULL & match_bits;
		r_cur_op->op_type = BMIP_EX_RECV;

		r_cur_op->num = num;
		r_cur_op->buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		r_cur_op->user_buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		r_cur_op->lengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		r_cur_op->alengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		r_cur_op->offsets = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);

		r_cur_op->use_barrier = use_barrier;
		r_cur_op->user_ptr = user_ptr;
		r_cur_op->comm_id = comm_id;

		for(i = 0 ; i < r_cur_op->num ; i++)
		{
			r_cur_op->buffers[i] = buffers[i];
			r_cur_op->lengths[i] = lengths[i];
		}

		/* if there is a pending local recv op already posted */
		p_send_op = bmip_locate_psendlocalop(r_cur_op);
		if(p_send_op)
		{
                	char c = 'e';

			/* copy contents */
			for(i = 0 ; i < r_cur_op->num ; i++)
			{
				memcpy(p_send_op->buffers[i], r_cur_op->buffers[i], r_cur_op->lengths[i]);
				p_send_op->alengths[i] = r_cur_op->lengths[i];
			}
			
                	qlist_add_tail(&r_cur_op->list, &bmip_done_ops);
                	qlist_add_tail(&p_send_op->list, &bmip_done_ops);

                	/* notify */
                	ret = write(workfds[1], &c, 1);
                	if(ret != 1)
                        	fprintf(stderr, "%s:%i error... could not write to ex pipe\n", __func__, __LINE__);

                	ret = write(workfds[1], &c, 1);
                	if(ret != 1)
                        	fprintf(stderr, "%s:%i error... could not write to ex pipe\n", __func__, __LINE__);

                	/* update the count */
                	server_num_done++;
                	server_num_done++;
		}
		/* other add this to the pedning local send list */
		else
		{
                	qlist_add_tail(&r_cur_op->list, &bmip_precvlocalop);
		}

		gen_mutex_unlock(&list_mutex);
		return 0;
	}

	if((cur_op = bmip_server_recv_pending(target, 0xffffffffULL & match_bits)))
	{
		cur_op->num = num;
		cur_op->buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->user_buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->lengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->alengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->offsets = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);

		cur_op->use_barrier = use_barrier;
		cur_op->user_ptr = user_ptr;
		cur_op->comm_id = comm_id;

		/* trace enter */
                bmip_trace_add(target, BMIP_SERVER_EX_RECV_DRIVER_START, cur_op->match_bits >> 3, cur_op->csid);	

		/* setup the md buffer meta info */
		for(i = 0 ; i < num ; i++)
		{
			cur_op->offsets[i] = bmip_data_get_offset(buffers[i]);
			cur_op->lengths[i] = lengths[i];

			/* this is driver allocated buffer */
			if(cur_op->offsets[i] < bmip_ex_space_length)
			{
				cur_op->buffers[i] = buffers[i];
				cur_op->user_buffers[i] = buffers[i]; 
			}
			/* this is an external buffer... setup an internal buffer */
			else
			{
				cur_op->buffers[i] = bmip_new_malloc(cur_op->lengths[i]); 
				cur_op->user_buffers[i] = buffers[i]; 
				cur_op->offsets[i] = bmip_data_get_offset(cur_op->buffers[i]);
			}
		}

		cur_op->context = (bmip_context_t *)bmip_safe_malloc(sizeof(bmip_context_t), __FILE__, __func__, __LINE__);

		if(use_barrier)
		{
			pthread_barrier_init(&cur_op->context->b, &bmip_context_attr, 2);
		}

		/* remove the op from the pending send list */
		qlist_del(&cur_op->list);

		/* init the put op */
		bmip_server_put_init(cur_op, -1, target);
	}
	else
	{
		cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
		memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

		/* setup op info */
		cur_op->target = target;
		cur_op->match_bits = 0xffffffffULL & match_bits;
		cur_op->op_type = BMIP_EX_RECV;

		/* trace enter */
                cur_op->csid = bmip_trace_get_index();
                bmip_trace_add(target, BMIP_SERVER_EX_RECV_USER_START, cur_op->match_bits >> 3, cur_op->csid);	

		cur_op->num = num;
		cur_op->buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->user_buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->lengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->alengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->offsets = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);

		cur_op->use_barrier = use_barrier;
		cur_op->user_ptr = user_ptr;
		cur_op->comm_id = comm_id;

		/* setup the md buffer meta info */
		for(i = 0 ; i < num ; i++)
		{
                        cur_op->offsets[i] = bmip_data_get_offset(buffers[i]);
                        cur_op->lengths[i] = lengths[i];

                        /* this is driver allocated buffer */
                        if(cur_op->offsets[i] < bmip_ex_space_length)
                        {
                                cur_op->buffers[i] = buffers[i];
                                cur_op->user_buffers[i] = buffers[i];
                        }
                        /* this is an external buffer... setup an internal buffer */
                        else
                        {
                                cur_op->buffers[i] = bmip_new_malloc(cur_op->lengths[i]); 
                                cur_op->user_buffers[i] = buffers[i];                            
                                cur_op->offsets[i] = bmip_data_get_offset(cur_op->buffers[i]);
                        }
                }

		cur_op->context = (bmip_context_t *)bmip_safe_malloc(sizeof(bmip_context_t), __FILE__, __func__, __LINE__);

		if(use_barrier)
		{
			pthread_barrier_init(&cur_op->context->b, &bmip_context_attr, 2);
		}

		/* set the callback */
		bmip_server_put_pending(cur_op, -1);

		/* add to the pending op list */
		qlist_add_tail(&cur_op->list, &bmip_server_pending_recv_ops);	
	}
	gen_mutex_unlock(&list_mutex);

	return ret;
}

int bmip_server_to_server_send(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
	int ret = PTL_OK;
	int i = 0;

	/* if this is a self send, don't actually send... just create the op structs and signal */
	if(local_pid.pid == op->target.pid && local_pid.nid == op->target.nid)
	{
                int ret = 0;
                bmip_portals_conn_op_t * s_cur_op = op;
                bmip_portals_conn_op_t * p_recv_op = NULL;

		/* if there is a pending local recv op already posted */
		p_recv_op = bmip_locate_precvlocalop(s_cur_op);
		if(p_recv_op)
		{
                	char c = 'e';

			/* copy contents */
			for(i = 0 ; i < s_cur_op->num ; i++)
			{
				memcpy(p_recv_op->buffers[i], s_cur_op->buffers[i], s_cur_op->lengths[i]);
				p_recv_op->alengths[i] = s_cur_op->lengths[i];
			}
			
                	qlist_add_tail(&s_cur_op->list, &bmip_done_ops);
                	qlist_add_tail(&p_recv_op->list, &bmip_done_ops);

                	/* notify */
                	ret = write(workfds[1], &c, 1);
                	if(ret != 1)
                        	fprintf(stderr, "%s:%i error... could not write to ex pipe\n", __func__, __LINE__);

                	ret = write(workfds[1], &c, 1);
                	if(ret != 1)
                        	fprintf(stderr, "%s:%i error... could not write to ex pipe\n", __func__, __LINE__);

                	/* update the count */
                	server_num_done++;
                	server_num_done++;
		}
		/* other add this to the pedning local send list */
		else
		{
                	qlist_add_tail(&s_cur_op->list, &bmip_psendlocalop);
		}
	}
	bmip_client_seq++;

	/* setup of the MDs */
        op->mdesc.start = op->buffers[0];
        op->mdesc.length = 0;
        op->mdesc.threshold = 2; /* send, ack */
        op->mdesc.options = PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	op->mdesc.eq_handle = bmi_eq;

        ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
                goto out;
        }

	/* setup the request info */
        bmip_ex_req_space[0] = op->num;
        for(i = 0 ; i < op->num ; i++)
        {
                /* set the msg length */
                bmip_ex_req_space[i + 1] = op->lengths[i];
        }

	/* send put request */
        ret = bmip_ptl_put(op->md, PTL_ACK_REQ, op->target, BMIP_PTL_INDEX, 0, op->match_bits | ex_req_mb | ex_req_put_mb, 0, op->num);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }

#ifdef BMIP_COUNT_ALL_EVENTS 
	op->put_push_rsp_wait_counter = 3;
#else
	op->put_push_rsp_wait_counter = 1;
#endif
	op->cur_function = bmip_server_to_server_put_local_put_wait;

	/* add to cur op list */
	qlist_add_tail(&op->list, &bmip_cur_ss_ops);
out:
	return 0;
}

int bmip_server_to_server_put_local_put_wait(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_SEND_START)
        {
                op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_SEND_END)
        {
                op->put_push_rsp_wait_counter--;
        }
        else if(etype == PTL_EVENT_ACK)
#else
        if(etype == PTL_EVENT_ACK)
#endif
        {
                op->put_push_rsp_wait_counter--;
        }

        /* if counter done... set next function */
        if(op->put_push_rsp_wait_counter == 0)
        {
                bmip_server_to_server_send_2(op, etype);
        }

        return 0;
}

int bmip_server_to_server_send_2(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
	int i = 0;
	int ret = PTL_OK;

	op->put_remote_get_wait_counter = 1;
	op->cur_function = bmip_server_to_server_put_remote_get_wait;

out:
	return 0;
}

int bmip_server_to_server_put_remote_get_wait(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_GET_START)
        {
                op->put_remote_get_wait_counter--;
        }
        else if(etype == PTL_EVENT_GET_END)
#else
        if(etype == PTL_EVENT_GET_END)
#endif
        {
                op->put_remote_get_wait_counter--;
        }

        /* if counter done... set next function */
        if(op->put_remote_get_wait_counter == 0)
        {
                bmip_server_to_server_send_3(op, etype);
        }

        return 0;
}

int bmip_server_to_server_send_3(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	op->cur_function = bmip_server_to_server_put_remote_put_wait;
	op->put_remote_put_wait_counter = 1;

	return 0;
}

int bmip_server_to_server_put_remote_put_wait(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_PUT_END)
        {
                op->put_remote_put_wait_counter--;
        }
        else if(etype == PTL_EVENT_PUT_END)
#else
        if(etype == PTL_EVENT_PUT_END)
#endif
        {
                op->put_remote_put_wait_counter--;
        }

        /* if counter done... set next function */
        if(op->put_remote_put_wait_counter == 0)
        {
                bmip_server_to_server_send_4(op, etype);
        }

        return 0;
}

int bmip_server_to_server_m0_transfer(bmip_portals_conn_op_t * op)
{
	int ret = 0;

	op->mdesc.threshold = 2; /* send, ack */
	op->mdesc.options = PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	op->mdesc.eq_handle = bmi_eq;
	op->mdesc.start = op->buffers[op->cur_ss_counter];
	op->mdesc.length = op->lengths[op->cur_ss_counter];
           
	ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
		goto out;
	}

	op->cur_ss_offset = bmip_ex_rsp_space[op->cur_ss_counter + 1];

	/* invoke the put */
	ret = bmip_ptl_put(op->md, PTL_ACK_REQ, op->target, BMIP_PTL_INDEX, 0, op->match_bits | ex_mb, op->cur_ss_offset, 0);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
		goto out;
	}    
        
        /* wait for ack */
#ifdef BMIP_COUNT_ALL_EVENTS 
	op->put_push_rsp_wait_counter = 3;
#else
	op->put_push_rsp_wait_counter = 1;
#endif
	op->cur_function = bmip_server_to_server_transfer_wait;

out:
	return 0;
}

int bmip_server_to_server_m1_transfer(bmip_portals_conn_op_t * op)
{
	int ret = 0;
	size_t rlength = bmip_ex_rsp_space[op->cur_ss_counter_limit + op->cur_ss_counter + 1];
	op->mdesc.threshold = 2; /* send, ack */
	op->mdesc.options = PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	op->mdesc.eq_handle = bmi_eq;
	op->mdesc.start = op->cur_ss_buffer;
	op->mdesc.length = rlength;
	op->cur_ss_buffer_inc = rlength;
        
	ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
		goto out;
	}

	op->cur_ss_offset = bmip_ex_rsp_space[op->cur_ss_counter + 1];

	/* invoke the put */
	ret = bmip_ptl_put(op->md, PTL_ACK_REQ, op->target, BMIP_PTL_INDEX, 0, op->match_bits | ex_mb, op->cur_ss_offset, 0);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* wait for ack */
#ifdef BMIP_COUNT_ALL_EVENTS 
	op->put_push_rsp_wait_counter = 3;
#else
	op->put_push_rsp_wait_counter = 1;
#endif
	op->cur_function = bmip_server_to_server_transfer_wait;

out:
	return 0;
}

int bmip_server_to_server_m2_transfer(bmip_portals_conn_op_t * op)
{
	int ret = 0;

	op->mdesc.threshold = 2; /* send, ack */
	op->mdesc.options = PTL_MD_OP_PUT | PTL_MD_MANAGE_REMOTE;
	op->mdesc.eq_handle = bmi_eq;
	op->mdesc.start = op->buffers[op->cur_ss_counter];
	op->mdesc.length = op->lengths[op->cur_ss_counter];
        
	ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* invoke the put */
	ret = bmip_ptl_put(op->md, PTL_ACK_REQ, op->target, BMIP_PTL_INDEX, 0, op->match_bits | ex_mb, op->cur_ss_offset, 0);
	if(ret != PTL_OK)
	{
		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
		goto out;
	}

	/* wait for ack */
#ifdef BMIP_COUNT_ALL_EVENTS 
	op->put_push_rsp_wait_counter = 3;
#else
	op->put_push_rsp_wait_counter = 1;
#endif
	op->cur_function = bmip_server_to_server_transfer_wait;

out:
	return 0;
}

int bmip_server_to_server_send_cleanup(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	op->match_bits = (op->match_bits >> 3);

        /* remove from the list */
        qlist_del(&op->list);

        /* release the context */

        /* if we use a barrier, signal the barrier */
        if(op->use_barrier)
        {
                pthread_barrier_wait(&op->context->b);
                pthread_barrier_destroy(&op->context->b);
                bmip_safe_free(op->context, __FILE__, __func__, __LINE__);

                /* free the op memory */
                bmip_safe_free(op->lengths, __FILE__, __func__, __LINE__);
                bmip_safe_free(op, __FILE__, __func__, __LINE__);
        }
        /* else, add to the done list and hit the cv */
        else
        {
                int i = 0;
                char c = 's';
                int ret = 0;

                for(i = 0 ; i < op->num ; i++)
                {
                        if(op->user_buffers[i] != op->buffers[i])
                        {
                                bmip_new_free(op->user_buffers[i]);
                        }
                }

		if(op->num > 0 && op->user_buffers)
		{
                	bmip_safe_free(op->user_buffers, __FILE__, __func__, __LINE__);
		}

                qlist_add_tail(&op->list, &bmip_done_ops);
                ret = write(workfds[1], &c, 1);
                if(ret != 1)
                        fprintf(stderr, "%s:%i error... could not write to put ex pipe\n", __func__, __LINE__);
                server_num_done++;
        }

        return 0;
}

int bmip_server_to_server_send_5_cleanup(void * op_, int etype)
{
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
	int ret = 0;

	/* cleanup */
	if(op->ss_mode == 0)
	{
        	ret = bmip_ptl_md_unlink(op->md);
        	if(ret != PTL_OK)
        	{
        		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
        		goto out;
        	}
        	op->md = PTL_HANDLE_INVALID;
        	op->cur_ss_counter++;

		if(op->cur_ss_counter < op->cur_ss_counter_limit)
		{
			bmip_server_to_server_m0_transfer(op);
		}
		else
		{
			bmip_server_to_server_send_cleanup(op, etype);
		}
	}
	else if(op->ss_mode == 1)
	{
		ret = bmip_ptl_md_unlink(op->md);
      		if(ret != PTL_OK)
 		{
       			bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
        		goto out;
		}	
		op->md = PTL_HANDLE_INVALID;

		op->cur_ss_buffer += op->cur_ss_buffer_inc;
		op->cur_ss_counter++;

		if(op->cur_ss_counter < op->cur_ss_counter_limit)
		{
			bmip_server_to_server_m1_transfer(op);
		}
		else
		{
			bmip_server_to_server_send_cleanup(op, etype);
		}
	}
	else if(op->ss_mode == 2)
	{
		ret = bmip_ptl_md_unlink(op->md);
       		if(ret != PTL_OK)
		{
       			bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
       			goto out;
      		}	
		op->md = PTL_HANDLE_INVALID;

		op->cur_ss_offset += op->lengths[op->cur_ss_counter];
		op->cur_ss_counter++;
		if(op->cur_ss_counter < op->cur_ss_counter_limit)
		{
			bmip_server_to_server_m2_transfer(op);
		}
		else
		{
			bmip_server_to_server_send_cleanup(op, etype);
		}
	}
out:
	return 0;
}

int bmip_server_to_server_send_4(void * op_, int etype)
{ 
        bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
	int ret = PTL_OK;

	/* cleanup */
	ret = bmip_ptl_md_unlink(op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req md", __FILE__, __func__, __LINE__);
                goto out;
        }	
	op->md = PTL_HANDLE_INVALID;
	
	int snum = bmip_ex_rsp_space[0];

	if(op->num == snum)
	{
		op->ss_mode = 0;
		op->cur_ss_counter_limit = op->num;
		op->cur_ss_counter = 0;

		/* set the remote offset */
		if(op->cur_ss_counter < op->cur_ss_counter_limit)
		{
			bmip_server_to_server_m0_transfer(op);
		}
	}
	else if(snum > op->num)
	{
		op->ss_mode = 1;
		op->cur_ss_counter_limit = snum;
		op->cur_ss_counter = 0;
		op->cur_ss_buffer = op->buffers[0];

		/* set the remote offset */
		if(op->cur_ss_counter < op->cur_ss_counter_limit)
		{
			bmip_server_to_server_m1_transfer(op);
		}
	}
	else
	{
		op->ss_mode = 2;
		op->cur_ss_counter_limit = op->num;
		op->cur_ss_counter = 0;
		op->cur_ss_offset = bmip_ex_rsp_space[1];

		/* set the remote offset */
		if(op->cur_ss_counter < op->cur_ss_counter_limit)
		{
			bmip_server_to_server_m2_transfer(op);
		}
	}
out:
	return ret;
}

int bmip_server_post_send(ptl_process_id_t target, int64_t match_bits, int num, void ** buffers, size_t * lengths, int use_barrier, void * user_ptr, int64_t comm_id)
{
	int ret = 0;
	bmip_portals_conn_op_t * cur_op = NULL;
	int i = 0;
	unsigned int s = 0;
	char is_target_server = 0;
	int64_t omb = match_bits;

	gen_mutex_lock(&list_mutex);

	/* if the target is another server, treat this as a client and make it active */
	is_target_server = bmip_addr_is_server(target);

	/* update the sequence and match bits */
        s = bmip_get_addr_seq(target, 2);
	match_bits = (match_bits << 3) | (s & 0x7);

	if(!(cur_op = bmip_server_send_pending(target, 0xffffffffULL & match_bits)) && !is_target_server)
	{
		cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
		memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

		/* setup op info */
		cur_op->target = target; 
		cur_op->match_bits = 0xffffffffULL & match_bits;
		cur_op->op_type = BMIP_EX_SEND;

		/* trace enter */
                cur_op->csid = bmip_trace_get_index();
                bmip_trace_add(target, BMIP_SERVER_EX_SEND_USER_START, cur_op->match_bits >> 3, cur_op->csid);	

		cur_op->num = num;
		cur_op->buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->user_buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->lengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->alengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->offsets = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);

		cur_op->use_barrier = use_barrier;
		cur_op->user_ptr = user_ptr;
		cur_op->comm_id = comm_id;

		/* setup the md buffer meta info */
		for(i = 0 ; i < num ; i++)
		{
                        cur_op->offsets[i] = bmip_data_get_offset(buffers[i]);
                        cur_op->lengths[i] = lengths[i];

                        /* this is driver allocated buffer */
                        if(cur_op->offsets[i] < bmip_ex_space_length)
                        {
                                cur_op->buffers[i] = buffers[i];
                                cur_op->user_buffers[i] = buffers[i];
                        }
                        /* this is an external buffer... setup an internal buffer */
                        else
                        {
                                cur_op->buffers[i] = bmip_new_malloc(cur_op->lengths[i]);
                                cur_op->user_buffers[i] = buffers[i];
                                cur_op->offsets[i] = bmip_data_get_offset(cur_op->buffers[i]);

				/* copy the user buffer contents to the internal buffer */
				memcpy(cur_op->buffers[i], cur_op->user_buffers[i], cur_op->lengths[i]);
                        }
		}

		cur_op->context = (bmip_context_t *)bmip_safe_malloc(sizeof(bmip_context_t), __FILE__, __func__, __LINE__);

		if(use_barrier)
		{
			pthread_barrier_init(&cur_op->context->b, &bmip_context_attr, 2);
		}

		/* set the callback */
		bmip_server_get_pending(cur_op, -1);

		qlist_add_tail(&cur_op->list, &bmip_server_pending_send_ops);
	}
	else
	{
		/* we don't have a cur op yet... make it */
		if(is_target_server)
		{
			cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
			memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));
		
			/* setup op info */
			cur_op->target = target; 
			cur_op->match_bits = 0xffffffffULL & match_bits;
			cur_op->op_type = BMIP_EX_SEND;
		}

		cur_op->buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->user_buffers = (void *)bmip_safe_malloc(sizeof(void *) * num, __FILE__, __func__, __LINE__);
		cur_op->lengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->alengths = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);
		cur_op->offsets = (size_t *)bmip_safe_malloc(sizeof(size_t) * num, __FILE__, __func__, __LINE__);

		cur_op->num = num;
		cur_op->use_barrier = use_barrier;
		cur_op->user_ptr = user_ptr;
		cur_op->comm_id = comm_id;

		/* trace enter */
                bmip_trace_add(target, BMIP_SERVER_EX_SEND_DRIVER_START, cur_op->match_bits >> 3, cur_op->csid);	

		/* setup the md buffer meta info */
		for(i = 0 ; i < num ; i++)
		{
                        cur_op->offsets[i] = bmip_data_get_offset(buffers[i]);
                        cur_op->lengths[i] = lengths[i];

                        /* this is driver allocated buffer */
                        if(cur_op->offsets[i] < bmip_ex_space_length)
                        {
                                cur_op->buffers[i] = buffers[i];
                                cur_op->user_buffers[i] = buffers[i];
                        }
                        /* this is an external buffer... setup an internal buffer */
                        else
                        {
                                cur_op->buffers[i] = bmip_new_malloc(cur_op->lengths[i]);
                                cur_op->user_buffers[i] = buffers[i];
                                cur_op->offsets[i] = bmip_data_get_offset(cur_op->buffers[i]);

                                /* copy the user buffer contents to the internal buffer */
                                memcpy((void*)cur_op->buffers[i], (void*)cur_op->user_buffers[i], cur_op->lengths[i]);
                        }
		}

		cur_op->context = (bmip_context_t *)bmip_safe_malloc(sizeof(bmip_context_t), __FILE__, __func__, __LINE__);

		if(use_barrier)
		{
			pthread_barrier_init(&cur_op->context->b, &bmip_context_attr, 2);
		}

		/* remove the op from the pending send list */
		if(!is_target_server)
		{
			qlist_del(&cur_op->list);
		}

		/* init the put op */
		if(is_target_server)
		{
			bmip_server_to_server_send(cur_op, -1);
		}
		else
		{
			bmip_server_get_init(cur_op, -1, target);
		}
	}
	gen_mutex_unlock(&list_mutex);

	return ret;
}

void bmip_server_wait_recv(bmip_context_t * context)
{
	pthread_barrier_wait(&context->b);
}

void bmip_server_wait_send(bmip_context_t * context)
{
	pthread_barrier_wait(&context->b);
}

int bmip_server_test_event_id(int ms_timeout, int nums, void ** user_ptrs, size_t * sizes, int64_t comm_id)
{
	int ret = 0;
	int found = 0;
	int num = 0;
	int firstpass = 1;
	struct timespec deadline;
	int l_server_num_done = 0;

	/* issue the timed wait */
	while(firstpass >= 0)
	{
		/* if not the first pass, wait for timeout before scanning list */
		if(firstpass == 0 && ms_timeout != 0)
		{
#ifdef BMIP_SPIN_WAIT
			int scount = 0;
			while(l_server_num_done == 0)
			{
				/* check for completed work */
				gen_mutex_lock(&list_mutex);
				if(server_num_done > 0)
					l_server_num_done = server_num_done;
				gen_mutex_unlock(&list_mutex);

				/* if we exceed the test count, break */
				if(scount > BMIP_MAX_TEST_COUNT)
				{
					break;
				}

				/* inc the test count */
				scount++;
#else
			{
				deadline.tv_sec = 1;
				deadline.tv_nsec = 0;
				{
					gen_mutex_lock(&sig_mutex);
					bmip_reinit_sighandler();
					ret = pselect(workfdmax + 1, &workfdset, NULL, NULL, &deadline, NULL);
					if(ret == 0)
					{
						deadline.tv_sec = 1;
						deadline.tv_nsec = 0;
						bmip_reinit_sighandler();
					}
					else if(ret < 0)
					{
						bmip_reinit_sighandler();
					}
					gen_mutex_unlock(&sig_mutex);
				}
#endif
			}
		}
		firstpass--;

		/* check the counter one last time */
		gen_mutex_lock(&list_mutex);

		/* update the counter if it is greater than zero */
		if(server_num_done > 0)
			l_server_num_done = server_num_done;

		gen_mutex_unlock(&list_mutex);

		if(l_server_num_done > 0)
		{
			/* protect the list */
			gen_mutex_lock(&list_mutex);

			/* check the done lists */
			bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
			qlist_for_each_entry_safe(n, nsafe, &bmip_done_ops, list)
			{
				if(n->comm_id == comm_id)
				{
					if(user_ptrs)
					{
						user_ptrs[num] = n->user_ptr;
					}

					/* find the size of the transfers */
					int j = 0;
					sizes[num] = 0;
					for(j = 0 ; j < n->num ; j++)
					{
						sizes[num] += n->lengths[j];
					}	

					qlist_del(&n->list);
					num++;

					/* cleanup op */
					if(n->context)
						bmip_safe_free(n->context, __FILE__, __func__, __LINE__);
					if(n->lengths)
						bmip_safe_free(n->lengths, __FILE__, __func__, __LINE__);
					if(n->alengths)
						bmip_safe_free(n->alengths, __FILE__, __func__, __LINE__);
					if(n->offsets)
						bmip_safe_free(n->offsets, __FILE__, __func__, __LINE__);
					if(n->buffers)
						bmip_safe_free(n->buffers, __FILE__, __func__, __LINE__);
					if(n)
						bmip_safe_free(n, __FILE__, __func__, __LINE__);

					break;
				}
			}

			/* reset the num done counter */	
			server_num_done--; 

			/* unlock the list lock */
			gen_mutex_unlock(&list_mutex);

			if(num > 0)
			{
				break;
			}
		}
	}

	return num;
}

int bmip_server_test_events(int ms_timeout, int nums, void ** user_ptrs, size_t * sizes, int64_t * comm_ids)
{
	int ret = 0;
	int found = 0;
	int num = 0;
	int firstpass = 1;
	struct timespec deadline;
	int l_server_num_done = 0;

	/* update the counter if it is greater than zero */
	gen_mutex_lock(&list_mutex);
	if(server_num_done > 0)
	{
		int ret_ = 0;
		char * b = NULL;
		if(server_num_done <= nums)
		{
			l_server_num_done = server_num_done;
			server_num_done = 0;
		}
		else
		{
			l_server_num_done = nums;
			server_num_done -= nums;
		}
		b = bmip_safe_malloc(l_server_num_done, __FILE__, __func__, __LINE__);
		ret_ = read(workfds[0], b, l_server_num_done);
		bmip_safe_free(b, __FILE__, __func__, __LINE__);
	}
	gen_mutex_unlock(&list_mutex);

	/* issue the timed wait */
	while(firstpass >= 0)
	{
		/* if not the first pass, wait for timeout before scanning list */
		if(firstpass == 0 && ms_timeout != 0 && l_server_num_done == 0)
		{
#ifdef BMIP_SPIN_WAIT
			int scount = 0;
			while(l_server_num_done == 0)
			{
				/* check for completed work */
				gen_mutex_lock(&list_mutex);
				if(server_num_done > 0)
					l_server_num_done = server_num_done;
				gen_mutex_unlock(&list_mutex);

				/* if we exceed the test count, break */
				if(scount > BMIP_MAX_TEST_COUNT)
				{
					break;
				}

				/* inc the test count */
				scount++;
#else
			{
				deadline.tv_sec = 1;
				deadline.tv_nsec = 0;
				{
					gen_mutex_lock(&sig_mutex);
					bmip_reinit_sighandler();
					ret = pselect(workfdmax + 1, &workfdset, NULL, NULL, &deadline, NULL);
					if(ret == 0)
					{
						deadline.tv_sec = 1;
						deadline.tv_nsec = 0;
						bmip_reinit_sighandler();
					}
					else if(ret < 0)
					{
						bmip_reinit_sighandler();
					}
					gen_mutex_unlock(&sig_mutex);
				}
#endif
			}
		}
		firstpass--;

		/* update the counter if it is greater than zero */
		gen_mutex_lock(&list_mutex);
		if(server_num_done > 0 && l_server_num_done == 0)
		{
                	char * b = NULL;
			int ret_ = 0;
			if(server_num_done <= nums)
			{
				l_server_num_done = server_num_done;
				server_num_done = 0;
			}
			else
			{
				l_server_num_done = nums;
				server_num_done -= nums;
			}
                	b = bmip_safe_malloc(l_server_num_done, __FILE__, __func__, __LINE__);
			ret_ = read(workfds[0], b, l_server_num_done);
                	bmip_safe_free(b, __FILE__, __func__, __LINE__);
		}

		if(l_server_num_done > 0)
		{
			/* check the done lists */
			bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
			qlist_for_each_entry_safe(n, nsafe, &bmip_done_ops, list)
			{
				user_ptrs[num] = n->user_ptr;
				comm_ids[num] = n->comm_id;

				/* find the size of the transfers */
				int j = 0;
				sizes[num] = 0;
					
				if(n->num == 1)
				{
					sizes[num] = n->ev_alength;
				}
				else
				{
					for(j = 0 ; j < n->num ; j++)
					{
						sizes[num] += n->lengths[j];
					}
				}

				qlist_del(&n->list);
				num++;

				if(n->op_type == BMIP_EX_SEND)
				{
                			bmip_trace_add(n->target, BMIP_SERVER_EX_SEND_USER_END, n->match_bits >> 3, n->csid);
				}
				else if(n->op_type == BMIP_EX_RECV)
				{
                			bmip_trace_add(n->target, BMIP_SERVER_EX_RECV_USER_END, n->match_bits >> 3, n->csid);
				}

				/* cleanup op */
				if(n->context)
					bmip_safe_free(n->context, __FILE__, __func__, __LINE__);
				if(n->lengths)
					bmip_safe_free(n->lengths, __FILE__, __func__, __LINE__);
				if(n->alengths)
					bmip_safe_free(n->alengths, __FILE__, __func__, __LINE__);
				if(n->offsets)
					bmip_safe_free(n->offsets, __FILE__, __func__, __LINE__);
				if(n->buffers)
					bmip_safe_free(n->buffers, __FILE__, __func__, __LINE__);
				if(n)
					bmip_safe_free(n, __FILE__, __func__, __LINE__);

				/* if we hit the list storage limit, break from the loop */
				if(num == l_server_num_done)
					break;
			}
		}
		gen_mutex_unlock(&list_mutex);

		/* break the loop if we found results */
		if(num > 0)
		{
			break;
		}
	}

	return num;
}

int bmip_server_test_unex_events(int ms_timeout, int nums, void ** umsgs, size_t * sizes, int64_t * tags, ptl_process_id_t * addrs)
{
	int ret = 0;
	int found = 0;
	int num = 0;
	int firstpass = 1;
	struct timespec deadline;
	int l_server_num_done = 0;

	/* update the counter if it is greater than zero */
	gen_mutex_lock(&list_mutex);
	if(server_num_unex_done > 0)
	{
        	char * b = NULL;
		int ret_ = 0;
		if(server_num_unex_done < nums)
		{
        		l_server_num_done = server_num_unex_done;
			server_num_unex_done = 0;
		}
		else
		{
        		l_server_num_done = nums;
			server_num_unex_done -= nums;
		}
        	b = bmip_safe_malloc(l_server_num_done, __FILE__, __func__, __LINE__);
        	ret_ = read(unexworkfds[0], b, l_server_num_done);
        	bmip_safe_free(b, __FILE__, __func__, __LINE__);
	}
	gen_mutex_unlock(&list_mutex);

	/* issue the timed wait */
	while(firstpass >= 0)
	{
		/* if not the first pass, wait for timeout before scanning list */
		if(firstpass == 0 && ms_timeout != 0 && l_server_num_done == 0)
		{
#ifdef BMIP_SPIN_WAIT
                        int scount = 0;
                        while(l_server_num_done == 0)
                        {
                                /* check for completed work */
                                gen_mutex_lock(&list_mutex);
                                if(server_num_done > 0)
                                        l_server_num_done = server_num_unex_done;
                                gen_mutex_unlock(&list_mutex);

                                /* if we exceed the test count, break */
                                if(scount > BMIP_MAX_TEST_COUNT)
                                {
                                        break;
                                }

                                /* inc the test count */
                                scount++;
#else
                        {
                                deadline.tv_nsec = 0;
                                deadline.tv_sec = 0;
                                {
                                        gen_mutex_lock(&sig_mutex);
					bmip_reinit_sighandler();
                                        ret = pselect(workfdmax + 1, &workfdset, NULL, NULL, &deadline, NULL);
					if(ret <= 0)	
					{
						bmip_reinit_sighandler();
					}
                                        gen_mutex_unlock(&sig_mutex);
                                }
#endif
			}
		}
		firstpass--;

		/* update the counter if it is greater than zero */
		gen_mutex_lock(&list_mutex);
		if(server_num_unex_done > 0 && l_server_num_done == 0)
		{
			int ret_ = 0;
                	char * b = NULL;
			if(server_num_unex_done <= nums)
			{
				l_server_num_done = server_num_unex_done;
				server_num_unex_done = 0;
			}
			else
			{
				l_server_num_done = nums;
				server_num_unex_done -= nums;
			}
                	b = bmip_safe_malloc(l_server_num_done, __FILE__, __func__, __LINE__);
                	ret_ = read(unexworkfds[0], b, l_server_num_done);
                	bmip_safe_free(b, __FILE__, __func__, __LINE__);
		}

		if(l_server_num_done > 0)
		{
			/* check the done lists */
			bmip_portals_conn_op_t * n = NULL, * nsafe = NULL;
			qlist_for_each_entry_safe(n, nsafe, &bmip_unex_done, list)
			{
				qlist_del(&n->list);

				/* copy unex msg */
				umsgs[num] = n->unex_msg;
				sizes[num] = n->unex_msg_len;
				tags[num] = n->match_bits;
				addrs[num] = n->target;

				/* free the msg event*/
				bmip_safe_free(n, __FILE__, __func__, __LINE__);

				/* update the index */
				num++;

				/* if we hit the list storage limit, break from the loop */
				if(num == l_server_num_done)
					break;
			}
		}

		gen_mutex_unlock(&list_mutex);

		if(num > 0)
		{
			break;
		}
	}
	return num;
}

int bmip_server_unex_cleanup(void * op_, int etype)
{
	char c='u';
	int ret = 0;
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	op->match_bits = (op->match_bits >> 3);
	
	/* delete list from pending queue and it to the done queue */
	qlist_del(&op->list);
	qlist_add_tail(&op->list, &bmip_unex_done);

	 /* unlink the request */
        ret = bmip_ptl_md_unlink(op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink unex md", __FILE__, __func__, __LINE__);
	}

	/* notify */
	ret = write(unexworkfds[1], &c, 1);
	if(ret != 1)
		fprintf(stderr, "%s:%i error... could not write to unex pipe\n", __func__, __LINE__);
        
	bmip_trace_add(op->target, BMIP_SERVER_UNEX_RECV_END, op->match_bits >> 3, op->csid);	

	/* update the count */
	server_num_unex_done++;

	return 0;
}

int bmip_server_unex_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

#ifdef BMIP_COUNT_ALL_EVENTS 
        if(etype == PTL_EVENT_SEND_START)
        {
		op->unex_wait_counter--;
        }
        else if(etype == PTL_EVENT_SEND_END)
        {
		op->unex_wait_counter--;
        }
        else if(etype == PTL_EVENT_REPLY_START)
#else
        if(etype == PTL_EVENT_REPLY_START)
#endif
        {
		op->unex_wait_counter--;
        }
        else if(etype == PTL_EVENT_REPLY_END)
        {
		op->unex_wait_counter--;
        }

	if(op->unex_wait_counter == 0)
	{
        	bmip_trace_add(op->target, PTL_LOCAL_GET_END, op->match_bits >> 3, op->ptlid);
		bmip_server_unex_cleanup(op, etype);
	}
	return 0;
}

int bmip_server_unex_pending(void * op_, int etype)
{
	int ret = 0;
	int roff = 0;
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

        /* setup of the MDs */
        op->mdesc.start = op->unex_msg;
        op->mdesc.length = op->unex_msg_len;
        op->mdesc.threshold = 2; /* send, ack */
        op->mdesc.options = PTL_MD_OP_GET;
        op->mdesc.eq_handle = bmi_eq;

        ret = bmip_ptl_md_bind(local_ni, op->mdesc, PTL_RETAIN, &op->md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not bind md", __FILE__, __func__, __LINE__);
        }
	
	op->ptlid = bmip_trace_get_index();
        bmip_trace_add(op->target, PTL_LOCAL_GET_START, op->match_bits >> 3, op->ptlid);

        ret = bmip_ptl_get(op->md, op->target, BMIP_PTL_INDEX, 0, (op->match_bits & 0xffffffffULL) | unex_mb, roff);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
        }

        /* set the callback */
#ifdef BMIP_COUNT_ALL_EVENTS 
	op->unex_wait_counter = 4;
#else
	op->unex_wait_counter = 2;
#endif
	op->cur_function = bmip_server_unex_wait;

	return 0;
}

void * bmip_server_monitor(void * args)
{
	int ret = PTL_OK;
	int etype = 0;
	int icount = 0;
	cpu_set_t cpuset;
	int s = 0;
	pthread_t thisThread;
      
	/* run the monitor */
	for(;;)
	{
		ptl_event_t ev;
		bmip_portals_conn_op_t * op = NULL;

		if(bmip_shutdown)
		{
			pthread_exit(NULL);
			return NULL;
		}

		/* get the event type */
		if(icount % 2 == 0)
		{
			etype = bmip_wait_ex_event(&ev);
			icount = 1;
		}
		else
		{
			etype = bmip_wait_unex_event(&ev);
			icount = 0;
		}

		if(etype < 0)
			continue;

		gen_mutex_lock(&list_mutex);

#ifdef BMIP_TRACE
		/* track all of the events */
		if(bmip_server_events_counter >= BMIP_EV_TRACE_LIMIT)
			bmip_server_events_counter = 0;
		int eindex = bmip_server_events_counter++;
		bmip_server_events[eindex] = etype;
		bmip_server_pid[eindex] = ev.initiator.pid;
		bmip_server_nid[eindex] = ev.initiator.nid;
		bmip_server_mb[eindex] = ev.match_bits;
#endif

		/* check if the message is an unexpected */
		if(ev.match_bits & unex_mb)
		{
			/* is this a server initiated unex send msg */
			if((op = bmip_unex_ss_op_in_progress(0xffffffffULL & ev.match_bits, ev.initiator)))
			{
				op->ev_list[op->ev_list_counter++] = etype;
				op->cur_function(op, etype);
			}
			/* if this is unex recv on the server */
			else if(!(op = bmip_unex_op_in_progress(0xffffffffULL & ev.match_bits, ev.initiator)))
			{
				/* start of the unex msg */
				if(etype == PTL_EVENT_PUT_END)
				{
					/* update sequence */
					//bmip_get_addr_seq(ev.initiator);

					/* create a new operation */
					bmip_portals_conn_op_t * cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
					memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

					/* create the unex msg space */
					cur_op->unex_msg_len = (size_t)ev.hdr_data;
					cur_op->unex_msg = bmip_safe_malloc((size_t)ev.hdr_data, __FILE__, __func__, __LINE__);
			
					/* update sequence */
					if((unex_server_traffic_mb & ev.match_bits) != 0)
					{
						bmip_get_addr_seq(ev.initiator, 1);
					}
					else
					{
						bmip_get_addr_seq(ev.initiator, 0);
					}	

					/* setup op info */
					cur_op->target = ev.initiator;
					cur_op->match_bits = 0xffffffffULL & ev.match_bits;
					cur_op->match_bits = cur_op->match_bits >> 3;
					cur_op->match_bits = cur_op->match_bits << 3;

					/* trace enter */
				        cur_op->csid = bmip_trace_get_index();
        				bmip_trace_add(ev.initiator, BMIP_SERVER_UNEX_RECV_START, cur_op->match_bits >> 3, cur_op->csid);

					/* set the callback */
					bmip_server_unex_pending(cur_op, etype);

					cur_op->ev_list[cur_op->ev_list_counter++] = etype;

					/* add to the pending op list */
					qlist_add_tail(&cur_op->list, &bmip_unex_pending);	
				}
			}
			else
			{
				op->ev_list[op->ev_list_counter++] = etype;
				op->cur_function(op, etype);
			}	
		}
		else
		{

		if((op = bmip_ss_op_in_progress(0xffffffffULL & ev.match_bits, ev.initiator)))
		{
			op->ev_list[op->ev_list_counter++] = etype;
			op->cur_function(op, etype);
		}
		/* if this event is not currently in progress, start to track it */
		else if(!(op = bmip_op_in_progress(0xffffffffULL & ev.match_bits, ev.initiator)))
		{
			/* this is a server recv */
			if(etype == PTL_EVENT_PUT_END && (ev.match_bits & ex_req_put_mb))
			{
				/* if request message */
				if(ev.match_bits & ex_req_mb)
				{ 
					//fprintf(stderr, "%s:%i recv event: req mb flag set\n", __func__, __LINE__);
					bmip_portals_conn_op_t * cur_op = NULL;

					/* check if the server requested this operation */
					if((cur_op = bmip_server_recv_pending(ev.initiator, 0xffffffffULL & ev.match_bits)))
					{
						/* remove the op from the pending send list */
						qlist_del(&cur_op->list);
 
						cur_op->rnum = (int)ev.hdr_data;

						cur_op->ev_list[cur_op->ev_list_counter++] = etype;

						/* trace enter */
				        	//cur_op->csid = bmip_trace_get_index();
        					bmip_trace_add(ev.initiator, BMIP_SERVER_EX_RECV_DRIVER_START, cur_op->match_bits >> 3, cur_op->csid);

						/* init the put op */
						bmip_server_put_init(cur_op, etype, ev.initiator);
					}
					/* op not request yet... queue the client response until the server requests */
					else
					{
						cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
						memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

						/* setup op info */
						cur_op->target = ev.initiator;
						cur_op->match_bits = 0xffffffffULL & ev.match_bits;
						cur_op->rnum = (int)ev.hdr_data;
						cur_op->op_type = BMIP_EX_RECV;

						/* trace enter */
				        	cur_op->csid = bmip_trace_get_index();
        					bmip_trace_add(ev.initiator, BMIP_SERVER_EX_RECV_USER_START, cur_op->match_bits >> 3, cur_op->csid);

						/* set the callback */
						bmip_server_put_pending(cur_op, etype);

						cur_op->ev_list[cur_op->ev_list_counter++] = etype;

						/* add to the pending op list */
						qlist_add_tail(&cur_op->list, &bmip_server_pending_recv_ops);	
					}
				}
			}
			/* this is a server send */
	                else if(etype == PTL_EVENT_PUT_END && (ev.match_bits & ex_req_get_mb))
			{
				/* if request message */
				if(ev.match_bits & ex_req_mb)
				{
					bmip_portals_conn_op_t * cur_op = NULL;

                                        if((cur_op = bmip_server_send_pending(ev.initiator, 0xffffffffULL & ev.match_bits)))
                                        {
						//fprintf(stderr, "%s:%i send event: local server init request\n", __func__, __LINE__);
                                                /* remove the op from the pending send list */
                                                qlist_del(&cur_op->list);

						cur_op->rnum = (int)ev.hdr_data;

						cur_op->ev_list[cur_op->ev_list_counter++] = etype;

						/* trace enter */
				        	//cur_op->csid = bmip_trace_get_index();
        					bmip_trace_add(ev.initiator, BMIP_SERVER_EX_SEND_DRIVER_START, cur_op->match_bits >> 3, cur_op->csid);

                                                /* init the put op */
                                                bmip_server_get_init(cur_op, etype, ev.initiator);
                                        }
                                        /* op not request yet... queue the client response until the server requests */
                                        else
                                        {
                                                cur_op = (bmip_portals_conn_op_t *)bmip_safe_malloc(sizeof(bmip_portals_conn_op_t), __FILE__, __func__, __LINE__);
						memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

                                                /* setup op info */
                                                cur_op->target = ev.initiator;
                                                cur_op->match_bits = 0xffffffffULL & ev.match_bits;
						cur_op->rnum = (int)ev.hdr_data;
						cur_op->op_type = BMIP_EX_SEND;

						/* trace enter */
				        	cur_op->csid = bmip_trace_get_index();
        					bmip_trace_add(ev.initiator, BMIP_SERVER_EX_SEND_USER_START, cur_op->match_bits >> 3, cur_op->csid);

                                                /* set the callback */
                                                bmip_server_get_pending(cur_op, etype);

						cur_op->ev_list[cur_op->ev_list_counter++] = etype;

                                                /* add to the pending op list */
                                                qlist_add_tail(&cur_op->list, &bmip_server_pending_send_ops);
                                        }
				}
			}
		}
		/* the event is in progress... advance the event sm */
		else
		{
			op->ev_alength = ev.mlength;
			op->ev_list[op->ev_list_counter++] = etype;
			op->cur_function(op, etype);
		}
		}
		gen_mutex_unlock(&list_mutex);
	}

out:
	pthread_exit(NULL);

	return NULL;
}

void * bmip_new_malloc(size_t length)
{
	void * mem = NULL;
	mem = bmip_data_get(length);
	if(mem >= bmip_ex_space && mem <= bmip_ex_space_end && (mem + length) <= bmip_ex_space_end)
		return mem;
	else
		fprintf(stderr, "%s:%i invalid mem = %p length = %lu\n", __func__, __LINE__, mem, length);
	return mem;
}

void bmip_new_free(void * mem)
{
	if(mem == NULL)
		return;
	if(mem >= bmip_ex_space && mem <= bmip_ex_space_end)
		bmip_data_release(mem);
	else
		fprintf(stderr, "%s:%i invalid mem = %p\n", __func__, __LINE__, mem);
	return;
}
