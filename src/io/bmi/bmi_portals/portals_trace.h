#ifndef PORTALS_TRACE_H
#define PORTALS_TRACE_H

#include <stdlib.h>
#include <stdint.h>

#include <portals/portals3.h>
#include <sys/utsname.h>
#include <sys/time.h>

#include "src/common/quicklist/quicklist.h"

typedef enum
{
	PTL_LOCAL_PUT_START=0,
	PTL_LOCAL_PUT_END,
	PTL_LOCAL_GET_START,
	PTL_LOCAL_GET_END,
	PTL_REMOTE_PUT_START,
	PTL_REMOTE_PUT_END,
	PTL_REMOTE_GET_START,
	PTL_REMOTE_GET_END,
	BMIP_CLIENT_EX_SEND_START,
	BMIP_CLIENT_EX_SEND_END,
	BMIP_CLIENT_EX_RECV_START,
	BMIP_CLIENT_EX_RECV_END,
	BMIP_CLIENT_UNEX_SEND_START,
	BMIP_CLIENT_UNEX_SEND_END,
	BMIP_SERVER_EX_SEND_USER_START,
	BMIP_SERVER_EX_SEND_USER_END,
	BMIP_SERVER_EX_RECV_USER_START,
	BMIP_SERVER_EX_RECV_USER_END,
	BMIP_SERVER_EX_SEND_DRIVER_START,
	BMIP_SERVER_EX_SEND_DRIVER_END,
	BMIP_SERVER_EX_RECV_DRIVER_START,
	BMIP_SERVER_EX_RECV_DRIVER_END,
	BMIP_SERVER_UNEX_RECV_START,
	BMIP_SERVER_UNEX_RECV_END,
} bmip_trace_marker_t;

typedef struct bmip_trace_event
{
        struct qlist_head list;
        ptl_process_id_t target;
        bmip_trace_marker_t etype;
        uint64_t time;
	uint64_t parent_id;
	uint64_t local_id;
} bmip_trace_event_t;

void bmip_trace_set_local_addr(ptl_process_id_t addr);
uint64_t bmip_trace_getcurtime(void);
void bmip_trace_add(ptl_process_id_t target, bmip_trace_marker_t etype, uint64_t pid, uint64_t lid);
void bmip_trace_dump_list(void);
uint64_t bmip_trace_get_index(void);
const char * bmip_trace_lookup_event(bmip_trace_marker_t etype);

#endif
