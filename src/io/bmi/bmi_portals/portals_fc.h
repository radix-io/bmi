#ifndef PORTALS_FC_H 
#define PORTALS_FC_H 

#define BMIP_MAX_NUM_CLIENTS 768  /*  768 clients (192 XT4 nodes or 64 XT5 nodes) */

#define BMIP_SLOT_MAX_SIZE (2<<10)
#define BMIP_SLOT_MASK (BMIP_SLOT_MAX_SIZE - 1)
 
#include <stdint.h>

/* portals includes */
#include <portals/portals3.h>
#include <sys/utsname.h>

/* connection node */
typedef struct bmip_fc_node
{
	/* id info */
	int64_t key;
	int32_t nid;
	int64_t pid;
	ptl_pid_t ptl_pid;

	/* large data attrs */
	int16_t slot;
	void * data;
	ptl_size_t length;
	ptl_handle_eq_t * eq_handle;
	ptl_md_t md;

	/* portals handles */
	int8_t valid;
	ptl_handle_md_t md_handle;
	ptl_handle_me_t me_handle;
	ptl_handle_ni_t ni_handle;

	/* match bits */
	ptl_match_bits_t mb;
	ptl_match_bits_t ib;
} bmip_fc_node_t;

/* fc mgmt funcs */
int bmip_fc_init(); 
int bmip_fc_destroy();
int bmip_fc_resv(int id);
int bmip_fc_release(int id);
int bmip_fc_check(int id);
int bmip_fc_resv_slot(int32_t nid, int32_t pid);
bmip_fc_node_t * bmip_fc_find_slot(int32_t nid, int32_t pid);

/* node mgmt funcs */
bmip_fc_node_t * bmip_fc_alloc_node();
void bmip_fc_free_node(void * node);

/* low level tree mgmt funcs */
int bmip_fc_node_comp(const void * a, const void * b);
int bmip_fc_node_ins(bmip_fc_node_t * node);
bmip_fc_node_t * bmip_fc_node_find(int64_t key);
bmip_fc_node_t * bmip_fc_node_del(int64_t key);
int64_t bmip_fc_hash(int32_t pid, int32_t nid);
int bmip_fc_tree_destroy();

int bmip_fc_setup_pp_me(bmip_fc_node_t * node);
int bmip_fc_setup_pp_md(bmip_fc_node_t * node);

#endif
