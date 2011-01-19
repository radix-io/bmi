#include "portals_wrappers.h"

/* network interface functions */
int bmip_ptl_init(int * num_interfaces)
{
	int ret = 0;

	ret = PtlInit(num_interfaces);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_ni_init(ptl_interface_t iface, ptl_pid_t pid, ptl_ni_limits_t * desired, ptl_ni_limits_t * actual, ptl_handle_ni_t * ni_handle)
{
	int ret = 0;

	ret = PtlNIInit(iface, pid, desired, actual, ni_handle);
	if(ret == PTL_IFACE_DUP)
        	ret = PTL_OK;
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_ni_fini(ptl_handle_ni_t ni_handle)
{
	int ret = 0;

	ret = PtlNIFini(ni_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

/* identification functions */
int bmip_ptl_get_id(ptl_handle_ni_t ni_handle, ptl_process_id_t * id)
{
	int ret = 0;

	ret = PtlGetId(ni_handle, id);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

/* match entry functions */
int bmip_ptl_me_attach(ptl_handle_ni_t ni_handle, ptl_pt_index_t pt_index, ptl_process_id_t match_id, ptl_match_bits_t match_bits,
		ptl_match_bits_t ignore_bits, ptl_unlink_t unlink_op, ptl_ins_pos_t position, ptl_handle_me_t * me_handle)
{
	int ret = 0;

	ret = PtlMEAttach(ni_handle, pt_index, match_id, match_bits, ignore_bits, unlink_op, position, me_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_me_insert(ptl_handle_me_t base, ptl_process_id_t match_id, ptl_match_bits_t match_bits, ptl_match_bits_t ignore_bits,
		ptl_unlink_t unlink_op, ptl_ins_pos_t position, ptl_handle_me_t * me_handle)
{
	int ret = 0;

	ret = PtlMEInsert(base, match_id, match_bits, ignore_bits, unlink_op, position, me_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_me_unlink(ptl_handle_me_t me_handle)
{
	int ret = 0;

	ret = PtlMEUnlink(me_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

/* memory descriptor functions */
int bmip_ptl_md_attach(ptl_handle_me_t me_handle, ptl_md_t md, ptl_unlink_t unlink_op, ptl_handle_md_t * md_handle)
{
	int ret = 0;

	ret = PtlMDAttach(me_handle, md, unlink_op, md_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_md_bind(ptl_handle_ni_t ni_handle, ptl_md_t md, ptl_unlink_t unlink_op, ptl_handle_md_t * md_handle)
{
	int ret = 0;

	ret = PtlMDBind(ni_handle, md, unlink_op, md_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_md_unlink(ptl_handle_md_t md_handle)
{
	int ret = 0;

	ret = PtlMDUnlink(md_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_md_update(ptl_handle_md_t md_handle, ptl_md_t * old_md, ptl_md_t * new_md, ptl_handle_eq_t eq_handle)
{
	int ret = 0;

	ret = PtlMDUpdate(md_handle, old_md, new_md, eq_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

/* event queue functions */
int bmip_ptl_eq_alloc(ptl_handle_ni_t ni_handle, ptl_size_t count, ptl_eq_handler_t eq_handler, ptl_handle_eq_t * eq_handle)
{
	int ret = 0;

	ret = PtlEQAlloc(ni_handle, count, eq_handler, eq_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_eq_free(ptl_handle_eq_t eq_handle)
{
	int ret = 0;

	ret = PtlEQFree(eq_handle);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_eq_get(ptl_handle_eq_t eq_handle, ptl_event_t * event)
{
	int ret = 0;

	ret = PtlEQGet(eq_handle, event);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_eq_wait(ptl_handle_eq_t eq_handle, ptl_event_t * event)
{
	int ret = 0;

	ret = PtlEQWait(eq_handle, event);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_eq_poll(ptl_handle_eq_t * eq_handles, int size, ptl_time_t timeout, ptl_event_t * event, int * which)
{
	int ret = 0;

	ret = PtlEQPoll(eq_handles, size, timeout, event, which);
	if(ret != PTL_OK && ret != PTL_EQ_EMPTY)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

/* access control functions */
int bmip_ptl_ac_entry(ptl_handle_ni_t ni_handle, ptl_ac_index_t ac_index, ptl_process_id_t match_id, ptl_uid_t uid,
		ptl_pt_index_t pt_index)
{
	int ret = 0;

	ret = PtlACEntry(ni_handle, ac_index, match_id, uid, pt_index);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

/* data movement functions */
int bmip_ptl_put(ptl_handle_md_t md_handle, ptl_ack_req_t ack_req, ptl_process_id_t target_id, ptl_pt_index_t pt_index,
		ptl_ac_index_t ac_index, ptl_match_bits_t match_bits, ptl_size_t remote_offset, ptl_hdr_data_t hdr_data)
{
	int ret = 0;

	ret = PtlPut(md_handle, ack_req, target_id, pt_index, ac_index, match_bits, remote_offset, hdr_data);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}

int bmip_ptl_get(ptl_handle_md_t md_handle, ptl_process_id_t target_id, ptl_pt_index_t pt_index, ptl_ac_index_t ac_index,
		ptl_match_bits_t match_bits, ptl_size_t remote_offset)
{
	int ret = 0;

	ret = PtlGet(md_handle, target_id, pt_index, ac_index, match_bits, remote_offset);
	if(ret != PTL_OK)
	{
		bmip_fprintf_err("error", ret, PtlErrorStr(ret), __FILE__, __func__, __LINE__);
	}

	return ret;
}
