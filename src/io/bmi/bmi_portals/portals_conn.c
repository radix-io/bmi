#include "portals_conn.h"

#include "portals_comm.h"
#include "portals_wrappers.h"
#include "portals_helpers.h"

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
#define BMIP_UNEX_SPACE (8 * (1<<10))
#define BMIP_UNEX_MSG_SIZE (8 * (1<<10) + 1)
static char bmip_unex_space[BMIP_UNEX_SPACE]; 
static const size_t bmip_unex_space_length = BMIP_UNEX_SPACE;

/* ex msg space */
#if 0
#ifdef BMIPSERVERMEM
#warning server mem
//static const size_t BMIP_SERVER_EX_SPACE = (1ul * 1024ul * 1024ul * 1024ul);
static const size_t BMIP_SERVER_EX_SPACE = (2ul * 1024ul * 1024ul * 1024ul);
//static const size_t BMIP_SERVER_EX_SPACE = (4ul * 1024ul * 1024ul * 1024ul);
//static const size_t BMIP_SERVER_EX_SPACE = (8ul * 1024ul * 1024ul * 1024ul);
#else
#warning client mem
static const size_t BMIP_SERVER_EX_SPACE = (16 * (1<<20));
#endif
#else
#ifdef BMIPSERVERMEM
#warning server mem
#define BMIP_SERVER_EX_SPACE (512ul * 1024ul * 1024ul)
//#define BMIP_SERVER_EX_SPACE (1ul * 1024ul * 1024ul * 1024ul)
//#define BMIP_SERVER_EX_SPACE (2ul * 1024ul * 1024ul * 1024ul)
//#define BMIP_SERVER_EX_SPACE (4ul * 1024ul * 1024ul * 1024ul)
//#define BMIP_SERVER_EX_SPACE (8ul * 1024ul * 1024ul * 1024ul)
#else
#warning client mem
#define BMIP_SERVER_EX_SPACE (16 * (1<<20))
#endif
#endif
#define BMIP_CLIENT_EX_SPACE (16 * (1<<20))
#define BMIP_EX_MSG_SIZE (8 * (1<<20) + 1)
#if 1
static char bmip_ex_space_buffer[BMIP_SERVER_EX_SPACE];
static void * bmip_ex_space = &bmip_ex_space_buffer[0];
static void * bmip_ex_space_end = &bmip_ex_space_buffer[BMIP_SERVER_EX_SPACE - 1];
#else
static void * bmip_ex_space = NULL;
static void * bmip_ex_space_end = NULL;
#endif
 
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

/* counters for the done ops */
static int server_num_done = 0;
static int server_num_unex_done = 0;

/* pending and done operation lists */
static QLIST_HEAD(bmip_unex_pending);
static QLIST_HEAD(bmip_unex_done);
static QLIST_HEAD(bmip_cur_ops);
static QLIST_HEAD(bmip_done_ops);
static QLIST_HEAD(bmip_server_pending_send_ops);
static QLIST_HEAD(bmip_server_pending_recv_ops);
static QLIST_HEAD(bmip_addr_seq_list);

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

unsigned int bmip_get_addr_seq(ptl_process_id_t pid)
{
	int found = 0;
	unsigned int s = 0;

	bmip_seq_t * n = NULL, * nsafe = NULL;
	qlist_for_each_entry_safe(n, nsafe, &bmip_addr_seq_list, list)
        {
		if(n->target.pid == pid.pid && n->target.nid == pid.nid)
		{	
			found = 1;
			n->counter++;
			s = n->counter;
			break;
		}
	}

	/* if we did not find the address, make a new seq number for it */
	if(!found)
	{
		bmip_seq_t * n = malloc(sizeof(bmip_seq_t));
		n->counter = 0;
		s = n->counter;
		n->target.pid = pid.pid;
		n->target.nid = pid.nid;
		qlist_add_tail(&n->list, &bmip_addr_seq_list);
	}

	return s;
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
	//fprintf(stderr, "%s:%i space = %p, space end = %p, len = %lli\n", __func__, __LINE__, bmip_ex_space, bmip_ex_space_end, len);

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
			free(n);
                        ecount++;
                }
		else if(n->eventid == PTL_EVENT_REPLY_END)
                {
                        qlist_del(&n->list);
			free(n);
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
#if 0
		if(etype == PTL_EVENT_SEND_START)
		{
			ecount++;
		}
#endif
		if(etype == PTL_EVENT_SEND_END)
		{
			ecount++;
		}
#if 0
		else if(etype == PTL_EVENT_REPLY_START)
		{
			ecount++;
		}
#endif
		else if(etype == PTL_EVENT_REPLY_END)
		{
			ecount++;
		}
		else
		{
			bmip_pending_event_t * pe = (bmip_pending_event_t *) malloc(sizeof(bmip_pending_event_t));
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
			free(n);
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
#if 0
                if(etype == PTL_EVENT_GET_START)
                {
                        ecount++;
                }
#endif
                if(etype == PTL_EVENT_GET_END)
                {
                        ecount++;
                }
                else
                {
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)malloc(sizeof(bmip_pending_event_t));
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
			free(n);
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
#if 0
                if(etype == PTL_EVENT_GET_START)
                {
                        ecount++;
                }
#endif
                if(etype == PTL_EVENT_GET_END)
                {
                        ecount++;
                }
                else
                {
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)malloc(sizeof(bmip_pending_event_t));
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
			free(n);
                        ecount++;
                }
                else if(n->eventid == PTL_EVENT_ACK)
                {
                        qlist_del(&n->list);
			free(n);
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
#if 0
                if(etype == PTL_EVENT_SEND_START)
                {
                        ecount++;
                }
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
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)malloc(sizeof(bmip_pending_event_t));
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
			free(n);
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

#if 0
                if(etype == PTL_EVENT_PUT_START)
                {
                        ecount++;
                }
#endif
                if(etype == PTL_EVENT_PUT_END)
                {
                        ecount++;
                }
                else
                {
                        bmip_pending_event_t * pe = (bmip_pending_event_t *)malloc(sizeof(bmip_pending_event_t));
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
        int tagseq = (tag << 3) | (bmip_client_seq & 0x7);

        bmip_client_seq++;
	
        /* handles and mdesc for each request */
        ptl_handle_md_t put_req_md = PTL_HANDLE_INVALID;
        ptl_md_t put_req_mdesc;

        /* match bits */
        ptl_match_bits_t mb = tagseq;
        //ptl_match_bits_t mb = tag;

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

        /* 1a: send put request */
        ret = bmip_ptl_put(put_req_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | unex_mb, 0, length);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }

	bmip_wait_local_put(&pelist);

	/* unlink the request */
	ret = bmip_ptl_md_unlink(put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req md", __FILE__, __func__, __LINE__);
                goto out;
        }	
	put_req_md = PTL_HANDLE_INVALID;

	bmip_wait_unex_remote_get(&pelist);

out:
        qlist_for_each_entry_safe(n, nsafe, &pelist, list)
        {
		qlist_del(&n->list);
		free(n);
	}

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
	int tagseq = (tag << 3) | (bmip_client_seq & 0x7);

	bmip_client_seq++;

	/* handles and mdesc for each request */
	ptl_handle_md_t put_req_md = PTL_HANDLE_INVALID;
	ptl_handle_md_t put_md = PTL_HANDLE_INVALID;
        ptl_md_t put_req_mdesc;
        ptl_md_t put_mdesc;

	/* match bits */
        ptl_match_bits_t mb = tagseq;
        //ptl_match_bits_t mb = tag;

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

	/* send put request */
        ret = bmip_ptl_put(put_req_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_req_mb | ex_req_put_mb, 0, num);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }


	bmip_wait_local_put(&pelist);

	bmip_wait_remote_get(&pelist);

	bmip_wait_remote_put(&pelist);
	
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

			/* invoke the put */
	        	ret = bmip_ptl_put(put_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset, 0);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
		
			/* wait for ack */
			bmip_wait_local_put(&pelist);

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

			/* invoke the put */
	        	ret = bmip_ptl_put(put_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset, 0);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
		
			/* wait for ack */
			bmip_wait_local_put(&pelist);

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


			/* invoke the put */
	        	ret = bmip_ptl_put(put_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset, 0);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not send ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
		
			/* wait for ack */
			bmip_wait_local_put(&pelist);

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
		free(n);
        }
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
        int tagseq = (tag << 3) | (bmip_client_seq & 0x7);

        bmip_client_seq++;

	/* handles and mdesc for each request */
	ptl_handle_md_t put_req_md = PTL_HANDLE_INVALID;
	ptl_handle_md_t put_md = PTL_HANDLE_INVALID;
        ptl_md_t put_req_mdesc;
        ptl_md_t put_mdesc;

	/* match bits */
        ptl_match_bits_t mb = tagseq;
        //ptl_match_bits_t mb = tag;

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

	/* send put request */
        ret = bmip_ptl_put(put_req_md, PTL_ACK_REQ, target, BMIP_PTL_INDEX, 0, mb | ex_req_mb | ex_req_get_mb, 0, num);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not send req ex msg", __FILE__, __func__, __LINE__);
                goto out;
        }

	/* wait for ack... 4 events */
	bmip_wait_local_put(&pelist);
	
	/* cleanup */
	ret = bmip_ptl_md_unlink(put_req_md);
        if(ret != PTL_OK)
        {
                bmip_fprintf("could not unlink req md", __FILE__, __func__, __LINE__);
                goto out;
        }	
	put_req_md = PTL_HANDLE_INVALID;

	bmip_wait_remote_put(&pelist);

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

			/* invoke the put */
	        	ret = bmip_ptl_get(put_md, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not recv ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
	
			/* wait for ack */
			bmip_wait_local_get(&pelist);
	
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
			int rlength = 0;

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

			/* invoke the put */
	        	ret = bmip_ptl_get(put_md, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not recv ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
	
			/* wait for ack */
			bmip_wait_local_get(&pelist);
	
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
		int rlength = bmip_ex_rsp_space[snum + 1];

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

			/* invoke the put */
	        	ret = bmip_ptl_get(put_md, target, BMIP_PTL_INDEX, 0, mb | ex_mb, offset);
        		if(ret != PTL_OK)
        		{
                		bmip_fprintf("could not recv ex msg", __FILE__, __func__, __LINE__);
	                	goto out;
        		}	
	
			/* wait for ack */
			bmip_wait_local_get(&pelist);
	
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
		free(n);
        }
	return ret;
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
	int nm = 0;
#if 0
	nm = 2;
#else
	nm = 1;
#endif
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
#if 0
        if(etype == PTL_EVENT_PUT_START)
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

	if(op->put_remote_put_wait_counter == 0)
	{
		bmip_server_put_cleanup(op, etype);
	}
	return 0;
}



int bmip_server_put_cleanup(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	/* remove from the list */
	qlist_del(&op->list);

	/* release the context */

	/* if we use a barrier, signal the barrier */
	if(op->use_barrier)
	{
		pthread_barrier_wait(&op->context->b);
		pthread_barrier_destroy(&op->context->b);
		free(op->context);	
	
		/* free the op memory */
		free(op->lengths);
		free(op);
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
		free(op->user_buffers);

		qlist_add_tail(&op->list, &bmip_done_ops);
		ret = write(workfds[1], &c, 1);
		if(ret != 1)
			fprintf(stderr, "%s:%i error... could not write to put ex pipe\n", __func__, __LINE__);
		server_num_done++;
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
	int nm = 0;
#if 0
	nm = 2;
#else
	nm = 1;
#endif
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

int bmip_server_get_remote_get_wait(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;
#if 0
        if(etype == PTL_EVENT_GET_START)
        {
                op->get_remote_get_wait_counter--;
        }
        else if(etype == PTL_EVENT_GET_END)
#else
        if(etype == PTL_EVENT_GET_END)
#endif
        {
                op->get_remote_get_wait_counter--;
        }

	if(op->get_remote_get_wait_counter == 0)
	{
		bmip_server_get_cleanup(op, etype);
	}
	return 0;
}

int bmip_server_get_cleanup(void * op_, int etype)
{
	bmip_portals_conn_op_t * op = (bmip_portals_conn_op_t *)op_;

	/* remove from the list */
	qlist_del(&op->list);

	/* release the context */
	if(op->use_barrier)
	{
		pthread_barrier_wait(&op->context->b);
		pthread_barrier_destroy(&op->context->b);
		free(op->context);
	
		/* free the op memory */
		free(op->lengths);
		free(op);
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
		free(op->user_buffers);

		qlist_add_tail(&op->list, &bmip_done_ops);
		ret = write(workfds[1], &c, 1);
		if(ret != 1)
			fprintf(stderr, "%s:%i error... could not write to get ex pipe\n", __func__, __LINE__);
		server_num_done++;
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

bmip_context_t * bmip_server_post_recv(ptl_process_id_t target, int64_t match_bits, int num, void ** buffers, size_t * lengths, int use_barrier, void * user_ptr, int64_t comm_id)
{
	bmip_portals_conn_op_t * cur_op = NULL;
	int i = 0;
	unsigned int s = 0;

	gen_mutex_lock(&list_mutex);

        /* update the sequence and match bits */
        s = bmip_get_addr_seq(target);
        match_bits = match_bits | (s & 0x7);

	if((cur_op = bmip_server_recv_pending(target, 0xffffffffULL & match_bits)))
	{
		cur_op->num = num;
		cur_op->buffers = (void *)malloc(sizeof(void *) * num);
		cur_op->user_buffers = (void *)malloc(sizeof(void *) * num);
		cur_op->lengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->alengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->offsets = (size_t *)malloc(sizeof(size_t) * num);

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

		cur_op->context = (bmip_context_t *)malloc(sizeof(bmip_context_t));

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
		cur_op = (bmip_portals_conn_op_t *)malloc(sizeof(bmip_portals_conn_op_t));
		memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

		/* setup op info */
		cur_op->target = target;
		cur_op->match_bits = 0xffffffffULL & match_bits;

		cur_op->num = num;
		cur_op->buffers = (void *)malloc(sizeof(void *) * num);
		cur_op->user_buffers = (void *)malloc(sizeof(void *) * num);
		cur_op->lengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->alengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->offsets = (size_t *)malloc(sizeof(size_t) * num);

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

		cur_op->context = (bmip_context_t *)malloc(sizeof(bmip_context_t));

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

	return cur_op->context;
}

bmip_context_t * bmip_server_post_send(ptl_process_id_t target, int64_t match_bits, int num, const void ** buffers, size_t * lengths, int use_barrier, void * user_ptr, int64_t comm_id)
{
	bmip_portals_conn_op_t * cur_op = NULL;
	int i = 0;
	unsigned int s = 0;

	gen_mutex_lock(&list_mutex);
	
	/* update the sequence and match bits */
        s = bmip_get_addr_seq(target);
	match_bits = match_bits | (s & 0x7);

	if(!(cur_op = bmip_server_send_pending(target, 0xffffffffULL & match_bits)))
	{
		cur_op = (bmip_portals_conn_op_t *)malloc(sizeof(bmip_portals_conn_op_t));
		memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));
	
		/* setup op info */
		cur_op->target = target; 
		cur_op->match_bits = 0xffffffffULL & match_bits;

		cur_op->num = num;
		cur_op->buffers = (const void *)malloc(sizeof(void *) * num);
		cur_op->user_buffers = (const void *)malloc(sizeof(void *) * num);
		cur_op->lengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->alengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->offsets = (size_t *)malloc(sizeof(size_t) * num);

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

		cur_op->context = (bmip_context_t *)malloc(sizeof(bmip_context_t));

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
		cur_op->buffers = (void *)malloc(sizeof(void *) * num);
		cur_op->user_buffers = (void *)malloc(sizeof(void *) * num);
		cur_op->lengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->alengths = (size_t *)malloc(sizeof(size_t) * num);
		cur_op->offsets = (size_t *)malloc(sizeof(size_t) * num);

		cur_op->num = num;
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

		cur_op->context = (bmip_context_t *)malloc(sizeof(bmip_context_t));

		if(use_barrier)
		{
			pthread_barrier_init(&cur_op->context->b, &bmip_context_attr, 2);
		}

		/* remove the op from the pending send list */
		qlist_del(&cur_op->list);

		/* init the put op */
		bmip_server_get_init(cur_op, -1, target);
	}
	gen_mutex_unlock(&list_mutex);

	return cur_op->context;
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
					if(ret > 0)
					{
						gen_mutex_lock(&list_mutex);
						if(FD_ISSET(workfds[0], &workfdset))
						{
							fprintf(stderr, "%s:%i ex event detected\n", __func__, __LINE__);
						}
						else if(FD_ISSET(unexworkfds[0], &workfdset))
						{
							fprintf(stderr, "%s:%i unex event detected\n", __func__, __LINE__);
						}
						else
						{
							fprintf(stderr, "%s:%i other event detected\n", __func__, __LINE__);
						}
						gen_mutex_unlock(&list_mutex);
					}
					else if(ret == 0)
					{
						fprintf(stderr, "%s:%i timeout\n", __func__, __LINE__);
						deadline.tv_sec = 1;
						deadline.tv_nsec = 0;
						bmip_reinit_sighandler();
					}
					else
					{
						fprintf(stderr, "%s:%i error = %i\n", __func__, __LINE__, ret);
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
					free(n->context);
					free(n->lengths);
					free(n->alengths);
					free(n->offsets);
					free(n->buffers);
					free(n);

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
		b = malloc(l_server_num_done);
		ret_ = read(workfds[0], b, l_server_num_done);
		free(b);
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
						fprintf(stderr, "%s:%i test events pselect() error, ret = %i\n", __func__, __LINE__, ret);
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
                	b = malloc(l_server_num_done);
			ret_ = read(workfds[0], b, l_server_num_done);
                	free(b);
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
				for(j = 0 ; j < n->num ; j++)
				{
					sizes[num] += n->lengths[j];
				}

				qlist_del(&n->list);
				num++;

				/* cleanup op */
				free(n->context);
				free(n->lengths);
				free(n->alengths);
				free(n->offsets);
				free(n->buffers);
				free(n);

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
        	b = malloc(l_server_num_done);
        	ret_ = read(unexworkfds[0], b, l_server_num_done);
        	free(b);
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
                	b = malloc(l_server_num_done);
                	ret_ = read(unexworkfds[0], b, l_server_num_done);
                	free(b);
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
				free(n);

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
      
#if 0 
	/* get the the thread ID */ 
	thisThread = pthread_self();

	/* set the thread affinity to core 0 */
	CPU_ZERO(&cpuset);
        CPU_SET(0, &cpuset);
        s = pthread_setaffinity_np(thisThread, sizeof(cpu_set_t), &cpuset);
        if (s != 0)
		fprintf(stderr, "%s:%i could not set the thread affinity\n", __func__, __LINE__);
#endif

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
			if(!(op = bmip_unex_op_in_progress(0xffffffffULL & ev.match_bits, ev.initiator)))
			{
				/* end of the unex msg */
				if(etype == PTL_EVENT_PUT_END)
				{
					/* update sequence */
					bmip_get_addr_seq(ev.initiator);

					/* create a new operation */
					bmip_portals_conn_op_t * cur_op = (bmip_portals_conn_op_t *)malloc(sizeof(bmip_portals_conn_op_t));
					memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

					/* create the unex msg space */
					cur_op->unex_msg_len = (size_t)ev.hdr_data;
					cur_op->unex_msg = malloc((size_t)ev.hdr_data);
				
					/* setup op info */
					cur_op->target = ev.initiator;
					cur_op->match_bits = 0xffffffffULL & ev.match_bits;
					cur_op->match_bits = cur_op->match_bits >> 3;
					cur_op->match_bits = cur_op->match_bits << 3;

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
		/* if this event is not currently in progress, start to track it */
		if(!(op = bmip_op_in_progress(0xffffffffULL & ev.match_bits, ev.initiator)))
		{
			/* this is a server recv */
			if(etype == PTL_EVENT_PUT_END && (ev.match_bits & ex_req_put_mb))
			{
				/* if request message */
				if(ev.match_bits & ex_req_mb)
				{ 
					bmip_portals_conn_op_t * cur_op = NULL;

					/* check if the server requested this operation */
					if((cur_op = bmip_server_recv_pending(ev.initiator, 0xffffffffULL & ev.match_bits)))
					{
						/* remove the op from the pending send list */
						qlist_del(&cur_op->list);
 
						cur_op->rnum = (int)ev.hdr_data;

						cur_op->ev_list[cur_op->ev_list_counter++] = etype;

						/* init the put op */
						bmip_server_put_init(cur_op, etype, ev.initiator);
					}
					/* op not request yet... queue the client response until the server requests */
					else
					{
						cur_op = (bmip_portals_conn_op_t *)malloc(sizeof(bmip_portals_conn_op_t));
						memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

						/* setup op info */
						cur_op->target = ev.initiator;
						cur_op->match_bits = 0xffffffffULL & ev.match_bits;
						cur_op->rnum = (int)ev.hdr_data;

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

                                        /* check if the server requested this operation */
                                        if((cur_op = bmip_server_send_pending(ev.initiator, 0xffffffffULL & ev.match_bits)))
                                        {
                                                /* remove the op from the pending send list */
                                                qlist_del(&cur_op->list);

						cur_op->rnum = (int)ev.hdr_data;

						cur_op->ev_list[cur_op->ev_list_counter++] = etype;

                                                /* init the put op */
                                                bmip_server_get_init(cur_op, etype, ev.initiator);
                                        }
                                        /* op not request yet... queue the client response until the server requests */
                                        else
                                        {
                                                cur_op = (bmip_portals_conn_op_t *)malloc(sizeof(bmip_portals_conn_op_t));
						memset(cur_op, 0, sizeof(bmip_portals_conn_op_t));

                                                /* setup op info */
                                                cur_op->target = ev.initiator;
                                                cur_op->match_bits = 0xffffffffULL & ev.match_bits;
						cur_op->rnum = (int)ev.hdr_data;

                                                /* set the callback */
                                                bmip_server_get_pending(cur_op, etype);

						cur_op->ev_list[cur_op->ev_list_counter++] = etype;

                                                /* add to the pending op list */
                                                qlist_add_tail(&cur_op->list, &bmip_server_pending_send_ops);
                                        }
				}
			}
			else
			{
				//fprintf(stderr, "%s:%i unaccounted for event... ERROR!\n", __func__, __LINE__);
				//fprintf(stderr, "%s:%i etype = %s, mb = %llx, nid (%i, %i)\n", __func__, __LINE__, bmip_ptl_ev_type(&ev), ev.match_bits, ev.initiator.nid, ev.initiator.pid);
			}
		}
		/* the event is in progress... advance the event sm */
		else
		{
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
		fprintf(stderr, "%s:%i invalid mem = %p length = %i\n", __func__, __LINE__, mem, length);
	return mem;
}

void bmip_new_free(void * mem)
{
	if(mem >= bmip_ex_space && mem <= bmip_ex_space_end)
		bmip_data_release(mem);
	else
		fprintf(stderr, "%s:%i invalid mem = %p\n", __func__, __LINE__, mem);
	return;
}
