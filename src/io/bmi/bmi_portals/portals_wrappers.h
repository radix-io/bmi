#ifndef PORTALS_WRAPPERS_H
#define PORTALS_WRAPPERS_H

#include <portals/portals3.h>
#include <sys/utsname.h>
#include "portals_helpers.h"

/* network interface functions */
int bmip_ptl_init(int * num_interfaces);
int bmip_ptl_ni_init(ptl_interface_t iface, ptl_pid_t pid, ptl_ni_limits_t * desired, ptl_ni_limits_t * actual, ptl_handle_ni_t * ni_handle);
int bmip_ptl_ni_fini(ptl_handle_ni_t ni_handle);

/* identification functions */
int bmip_ptl_get_id(ptl_handle_ni_t ni_handle, ptl_process_id_t * id);

/* match entry functions */
int bmip_ptl_me_attach(ptl_handle_ni_t ni_handle, ptl_pt_index_t pt_index, ptl_process_id_t match_id, ptl_match_bits_t match_bits,
        ptl_match_bits_t ignore_bits, ptl_unlink_t unlink_op, ptl_ins_pos_t position, ptl_handle_me_t * me_handle);
int bmip_ptl_me_insert(ptl_handle_me_t base, ptl_process_id_t match_id, ptl_match_bits_t match_bits, ptl_match_bits_t ignore_bits,
        ptl_unlink_t unlink_op, ptl_ins_pos_t position, ptl_handle_me_t * me_handle);
int bmip_ptl_me_unlink(ptl_handle_me_t me_handle);

/* memory descriptor functions */
int bmip_ptl_md_attach(ptl_handle_me_t me_handle, ptl_md_t md, ptl_unlink_t unlink_op, ptl_handle_md_t * md_handle);
int bmip_ptl_md_bind(ptl_handle_ni_t ni_handle, ptl_md_t md, ptl_unlink_t unlink_op, ptl_handle_md_t * md_handle);
int bmip_ptl_md_unlink(ptl_handle_md_t md_handle);
int bmip_ptl_md_update(ptl_handle_md_t md_handle, ptl_md_t * old_md, ptl_md_t * new_md, ptl_handle_eq_t eq_handle);

/* event queue functions */
int bmip_ptl_eq_alloc(ptl_handle_ni_t ni_handle, ptl_size_t count, ptl_eq_handler_t eq_handler, ptl_handle_eq_t * eq_handle);
int bmip_ptl_eq_free(ptl_handle_eq_t eq_handle);
int bmip_ptl_eq_get(ptl_handle_eq_t eq_handle, ptl_event_t * event);
int bmip_ptl_eq_wait(ptl_handle_eq_t eq_handle, ptl_event_t * event);
int bmip_ptl_eq_poll(ptl_handle_eq_t * eq_handles, int size, ptl_time_t timeout, ptl_event_t * event, int * which);

/* access control functions */
int bmip_ptl_ac_entry(ptl_handle_ni_t ni_handle, ptl_ac_index_t ac_index, ptl_process_id_t match_id, ptl_uid_t uid,
        ptl_pt_index_t pt_index);

/* data movement functions */
int bmip_ptl_put(ptl_handle_md_t md_handle, ptl_ack_req_t ack_req, ptl_process_id_t target_id, ptl_pt_index_t pt_index,
        ptl_ac_index_t ac_index, ptl_match_bits_t match_bits, ptl_size_t remote_offset, ptl_hdr_data_t hdr_data);
int bmip_ptl_get(ptl_handle_md_t md_handle, ptl_process_id_t target_id, ptl_pt_index_t pt_index, ptl_ac_index_t ac_index,
        ptl_match_bits_t match_bits, ptl_size_t remote_offset);

#endif
