#ifndef PORTALS_CONN_H
#define PORTALS_CONN_H

#include <stdlib.h>
#include <stdint.h>

#include <portals/portals3.h>
#include <sys/utsname.h>

#include "src/common/quicklist/quicklist.h"
#include "src/io/bmi/bmi.h"

#define BMIP_USE_CVTEST 0
#define BMIP_USE_BARRIER 1

#define BMIP_EX_SEND 1
#define BMIP_UNEX_SEND 2
#define BMIP_EX_RECV 3
#define BMIP_UNEX_RECV 4

#define BMIP_UNEX_SS_LOCK_FREE 10
#define BMIP_UNEX_SS_LOCK_HELD 11

#define BMIP_MAX_LISTIO 1025

#define BMIP_EV_LIMIT 128

typedef struct bmip_seq
{
	struct qlist_head list;
	ptl_process_id_t target;
	unsigned int counter;
	char server;
} bmip_seq_t;

typedef struct bmip_pending_event
{
	struct qlist_head list;
	int eventid;
	ptl_event_t event; 
} bmip_pending_event_t;

typedef struct bmip_context
{
	pthread_barrier_t b;
	bmi_op_id_t op;
	bmi_context_id context;
	bmi_error_code_t error;
} bmip_context_t;

typedef struct bmip_portals_conn_op
{
	struct qlist_head list;

	/* tree data */
	int32_t key;

	size_t ev_alength;

	/* op data */	
	int8_t op_type;
	void ** buffers;
	void ** user_buffers;
	size_t * lengths;
	size_t * alengths;
	size_t * offsets;
	int num; 
	int rnum; 
	size_t req_buffer[BMIP_MAX_LISTIO];
	int64_t match_bits;
	bmip_context_t * context;
	void * user_ptr;
	int64_t comm_id;
	int use_barrier;
	void * unex_msg;
	size_t unex_msg_len;

	ptl_process_id_t target;
	ptl_md_t mdesc;
        ptl_handle_md_t md;

	/* list of events */
	int ev_list[BMIP_EV_LIMIT];
	int ev_list_counter;

	/* operation state machine data storage */
	int (*cur_function)(void * op, int etype);
	int put_fetch_req_wait_counter;
	int put_push_rsp_wait_counter;
	int put_remote_put_wait_counter;
	int get_local_rsp_wait_counter;
        int get_remote_get_wait_counter;
        int unex_wait_counter;

	int cur_ss_counter;
	int cur_ss_counter_limit;
	size_t cur_ss_offset;
	char ss_mode;
	void * cur_ss_buffer;
	size_t cur_ss_buffer_inc;
	int put_remote_get_wait_counter;

	char * unex_buffer;
	size_t unex_buffer_length;
	ptl_match_bits_t unexmb;

	/* trace events */
	uint64_t csid;
	uint64_t ptlid;

} bmip_portals_conn_op_t;

/* connection setup and shutdown */
int bmip_init(int pid);
int bmip_finalize(void);

/* connection info queries */
int bmip_get_ptl_pid(void);
int bmip_get_ptl_nid(void);

/* eq mgmt */
int bmip_setup_eqs(int is_server);
int bmip_dest_eqs(void);

int bmip_wait_unex_event(ptl_event_t * ev);

/* data transfer ops */
int bmip_unex_msg_send(ptl_process_id_t target_pid, void * buffer, size_t len, int64_t tag, ptl_handle_md_t * md);

/* server state machine code */
int bmip_server_put_local_get_req_info(void * op_, int etype);
int bmip_server_put_local_get_req_wait(void * op_, int etype);
int bmip_server_put_local_put_rsp_info(void * op_, int etype);
int bmip_server_put_local_put_wait(void * op_, int etype);
int bmip_server_put_remote_put_wait(void * op_, int etype);
int bmip_server_put_cleanup(void * op_, int etype);
int bmip_server_put_pending(void * op_, int etype);
int bmip_server_put_init(void * op_, int etype, ptl_process_id_t pid);

int bmip_server_get_local_put_rsp_info(void * op_, int etype);
int bmip_server_get_local_put_wait(void * op_, int etype);
int bmip_server_get_remote_get(void * op_, int etype);
int bmip_server_get_remote_get_wait(void * op_, int etype);
int bmip_server_get_cleanup(void * op_, int etype);
int bmip_server_get_pending(void * op_, int etype);
int bmip_server_get_init(void * op_, int etype, ptl_process_id_t op_pid);

int bmip_server_post_unex_send(ptl_process_id_t target, int num, void * buffer, size_t length, int tag, void * user_ptr, int64_t comm_id);
int bmip_server_unex_send_put_local_put_wait(void * op_, int etype);
int bmip_server_unex_send_get_remote_get_wait(void * op_, int etype);
int bmip_server_unex_send_put_remote_put(bmip_portals_conn_op_t * cur_op, int etype);
int bmip_server_unex_send_cleanup(void * op_, int etype);

void * bmip_server_monitor(void * args);
int bmip_server_post_recv(ptl_process_id_t target, int64_t match_bits, int num, void ** buffers, size_t * lengths, int use_barrier, void * user_ptr, int64_t comm_id);
void bmip_server_wait_recv(bmip_context_t * context);

int bmip_server_post_send(ptl_process_id_t target, int64_t match_bits, int num, void ** buffers, size_t * lengths, int use_barrier, void * user_ptr, int64_t comm_id);
void bmip_server_wait_send(bmip_context_t * context);

void * bmip_new_malloc(size_t len);
void bmip_new_free(void * mem);

void bmip_monitor_shutdown(void);

/* client functions */
int bmip_client_send(ptl_process_id_t target, int num, void ** buffers, size_t * lengths, int tag);
int bmip_client_recv(ptl_process_id_t target, int num, void ** buffers, size_t * lengths, int tag);

int bmip_server_test_events(int ms_timeout, int nums, void ** contexts, size_t * sizes, int64_t * comm_ids);
int bmip_server_test_unex_events(int ms_timeout, int nums, void ** umsgs, size_t * sizes, int64_t * tags, ptl_process_id_t * addrs);
int bmip_client_unex_send(ptl_process_id_t target, int num, void * buffer, size_t length, int tag);

int bmip_get_max_ex_msg_size(void);
int bmip_get_max_unex_msg_size(void);

int bmip_server_put_remote_put(void * op_, int etype);

int bmip_server_test_event_id(int ms_timeout, int nums, void ** user_ptrs, size_t * sizes, int64_t comm_id);

int bmip_is_local_addr(ptl_process_id_t pid);
ptl_process_id_t bmip_get_ptl_id(void);

void bmip_allocate_client_mem(void);
void bmip_allocate_server_mem(void);
void bmip_free_client_mem(void);
void bmip_free_server_mem(void);

int bmip_server_to_server_put_local_put_wait(void * op_, int etype);
int bmip_server_to_server_put_remote_get_wait(void * op_, int etype);
int bmip_server_to_server_put_remote_put_wait(void * op_, int etype);

int bmip_server_to_server_send(void * op, int etype);
int bmip_server_to_server_send_2(void * op, int etype);
int bmip_server_to_server_send_3(void * op, int etype);
int bmip_server_to_server_send_4(void * op, int etype);
int bmip_server_to_server_send_5_cleanup(void * op, int etype);
int bmip_server_unex_start_op(void * op_, int etype);
#endif
