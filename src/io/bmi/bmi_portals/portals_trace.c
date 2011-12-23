#include "portals_trace.h"

#include <src/common/gen-locks/gen-locks.h>
#include <src/common/quicklist/quicklist.h>

#include <stdio.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

static QLIST_HEAD(bmip_trace_list);
static int bmip_trace_list_count = 0;
static gen_mutex_t bmip_trace_mutex = GEN_MUTEX_INITIALIZER;
static gen_mutex_t bmip_trace_index_mutex = GEN_MUTEX_INITIALIZER;
static uint64_t bmip_trace_index = 0;
static ptl_process_id_t trace_local_addr;

void bmip_trace_set_local_addr(ptl_process_id_t addr)
{
	trace_local_addr = addr;
}

const char * bmip_trace_lookup_event(bmip_trace_marker_t etype)
{
	switch(etype)
	{
        	case PTL_LOCAL_PUT_START:
			return "PTL_LOCAL_PUT_START";
        	case PTL_LOCAL_PUT_END:
			return "PTL_LOCAL_PUT_END";
	        case PTL_LOCAL_GET_START:
			return "PTL_LOCAL_GET_START";
        	case PTL_LOCAL_GET_END:
			return "PTL_LOCAL_GET_END";
        	case PTL_REMOTE_PUT_START:
			return "PTL_REMOTE_PUT_START";
        	case PTL_REMOTE_PUT_END:
			return "PTL_REMOTE_PUT_END";
	        case PTL_REMOTE_GET_START:
			return "PTL_REMOTE_GET_START";
        	case PTL_REMOTE_GET_END:
			return "PTL_REMOTE_GET_END";
	        case BMIP_CLIENT_EX_SEND_START:
			return "BMIP_CLIENT_EX_SEND_START";
        	case BMIP_CLIENT_EX_SEND_END:
			return "BMIP_CLIENT_EX_SEND_END";
	        case BMIP_CLIENT_EX_RECV_START:
			return "BMIP_CLIENT_EX_RECV_START";
        	case BMIP_CLIENT_EX_RECV_END:
			return "BMIP_CLIENT_EX_RECV_END";
	        case BMIP_CLIENT_UNEX_SEND_START:
			return "BMIP_CLIENT_UNEX_SEND_START";
        	case BMIP_CLIENT_UNEX_SEND_END:
			return "BMIP_CLIENT_UNEX_SEND_END";
 		case BMIP_SERVER_EX_SEND_USER_START:
			return "BMIP_SERVER_EX_SEND_USER_START";
        	case BMIP_SERVER_EX_SEND_USER_END:
			return "BMIP_SERVER_EX_SEND_USER_END";
	        case BMIP_SERVER_EX_RECV_USER_START:
			return "BMIP_SERVER_EX_RECV_USER_START";
        	case BMIP_SERVER_EX_RECV_USER_END:
			return "BMIP_SERVER_EX_RECV_USER_END";
 		case BMIP_SERVER_EX_SEND_DRIVER_START:
			return "BMIP_SERVER_EX_SEND_DRIVER_START";
        	case BMIP_SERVER_EX_SEND_DRIVER_END:
			return "BMIP_SERVER_EX_SEND_DRIVER_END";
	        case BMIP_SERVER_EX_RECV_DRIVER_START:
			return "BMIP_SERVER_EX_RECV_DRIVER_START";
        	case BMIP_SERVER_EX_RECV_DRIVER_END:
			return "BMIP_SERVER_EX_RECV_DRIVER_END";
	        case BMIP_SERVER_UNEX_RECV_START:
			return "BMIP_SERVER_UNEX_RECV_START";
        	case BMIP_SERVER_UNEX_RECV_END:
			return "BMIP_SERVER_UNEX_RECV_END";
		default:
			return "UNKNOWN";
	};
	return NULL;
}

uint64_t bmip_trace_get_index(void)
{
	uint64_t t = 0;

#ifdef BMIP_ENABLE_TRACE
        gen_mutex_lock(&bmip_trace_index_mutex);
	t = bmip_trace_index++;
        gen_mutex_unlock(&bmip_trace_index_mutex);
#endif
	
	return t;
}
 
uint64_t bmip_trace_getcurtime(void)
{
        uint64_t t = 0;
        struct timeval cur;

	/* get the current time */
        gettimeofday(&cur, NULL);

	/* convert the time */
        t = (uint64_t)((cur.tv_sec * 1000000) + cur.tv_usec);

        return t;
}

void bmip_trace_add(ptl_process_id_t target, bmip_trace_marker_t etype, uint64_t pid, uint64_t lid)
{
#ifdef BMIP_ENABLE_TRACE
	/* allocate a new event */
        bmip_trace_event_t * e = (bmip_trace_event_t *)malloc(sizeof(bmip_trace_event_t));

	/* populate the event fields */
        e->etype = etype;
        e->local_id = lid;
        e->parent_id = pid;
        e->target = target;
        e->time = bmip_trace_getcurtime();

	/* add the event to the list */
        gen_mutex_lock(&bmip_trace_mutex);
        qlist_add_tail(&e->list, &bmip_trace_list);
	bmip_trace_list_count++;

	/* dump records */
	if(bmip_trace_list_count > 5000)
	{
		bmip_trace_dump_list();
		bmip_trace_list_count = 0;
	}
        gen_mutex_unlock(&bmip_trace_mutex);
#endif
}

void bmip_trace_dump_list(void)
{
#ifdef BMIP_ENABLE_TRACE
        bmip_trace_event_t * e = NULL, * esafe = NULL;
	char fname[256];
	FILE * fd = NULL;

	uint64_t key = (trace_local_addr.nid << 10) | trace_local_addr.pid;

	/* open trace file */
	sprintf(fname, "./bmip-ptl-trace-nid%05i-pid%03i.txt", trace_local_addr.nid, trace_local_addr.pid);
	fd = fopen(fname, "a+");

	/* iterate over the events */
        qlist_for_each_entry_safe(e, esafe, &bmip_trace_list, list)
        {
		/* event type */
                const char * ename = bmip_trace_lookup_event(e->etype);

		/* write out the trace info */
                fprintf(fd, "t=%lu event=%s key=%lu target.nid=%05u target.pid=%03u local.nid=%05u local.pid=%03u parentid=%lu localid=%lu\n", e->time, ename, key, e->target.nid, e->target.pid, trace_local_addr.nid, trace_local_addr.pid, e->parent_id, e->local_id);

		/* remove and cleanup */
		qlist_del(&e->list);
		free(e);
        }

	/* close trace file */
	fclose(fd);
#endif
}
