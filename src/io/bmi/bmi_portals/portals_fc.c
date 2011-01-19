#include "portals_fc.h"

#include <stdlib.h>

#define _GNU_SOURCE
#include <search.h>

static int * bmip_fc_credits = NULL;
static int bmip_fc_num_slots_used = 0; /* we only inc this... we assume we don't lose connections */
static const ptl_pt_index_t local_ptl_index = 37;
static void * bmip_fc_conn_tree = NULL;

bmip_fc_node_t * bmip_fc_alloc_node()
{   
	return (bmip_fc_node_t *)malloc(sizeof(bmip_fc_node_t));
}

void bmip_fc_free_node(void * node)
{
	free(node);
}

int bmip_fc_node_comp(const void * a, const void * b)
{
	bmip_fc_node_t * a_node = (bmip_fc_node_t *)a;
	bmip_fc_node_t * b_node = (bmip_fc_node_t *)b;

	if(a_node->key < b_node->key)
	{
		return -1;
	}
	else if(a_node->key > b_node->key)
	{
		return 1;
	}

	return 0;
}

int bmip_fc_node_ins(bmip_fc_node_t * node)
{
	tsearch(node, &bmip_fc_conn_tree, bmip_fc_node_comp);
}

bmip_fc_node_t * bmip_fc_node_find(int64_t key)
{
	bmip_fc_node_t node;
	node.key = key;

	return tfind(&node, &bmip_fc_conn_tree, bmip_fc_node_comp);
}

bmip_fc_node_t * bmip_fc_node_del(int64_t key)
{
	bmip_fc_node_t node;
	node.key = key;

	return (bmip_fc_node_t *)tdelete(&node, &bmip_fc_conn_tree, bmip_fc_node_comp);
}

int64_t bmip_fc_hash(int32_t pid, int32_t nid)
{
	int64_t key = 0;
	key |=  nid;
	key =  (key << 32) | pid;

	key = (~key) + (key << 21);
	key = key ^ (key >> 24);
	key = (key + (key << 3)) + (key << 8);
	key = key ^ (key >> 14);
	key = (key + (key << 2)) + (key << 4);
	key = key ^ (key >> 28);
	key = key + (key << 31);

	return key;
}

int bmip_fc_init()
{
	int i = 0;

	bmip_fc_credits = (int *)malloc(sizeof(int) * BMIP_MAX_NUM_CLIENTS);

	/* setup the credit ds */
	for(i = 0 ; i < BMIP_MAX_NUM_CLIENTS ; i++)
	{
		bmip_fc_credits[i] = 1;
	}

	return 0;
}

int bmip_fc_destroy()
{
	/* cleanup the credit ds */
	free(bmip_fc_credits);
	bmip_fc_credits = NULL;

	return 0;
}

int bmip_fc_resv(int id)
{
	/* allocate space */
	bmip_fc_credits[id] = 0;
}

int bmip_fc_release(int id)
{
	/* release the space */
	bmip_fc_credits[id] = 1;
}

int bmip_fc_check(int id)
{
	return bmip_fc_credits[id];
}

int bmip_fc_resv_slot(int32_t nid, int32_t pid)
{
	int64_t key = bmip_fc_hash(nid, pid);
	bmip_fc_node_t * node = bmip_fc_alloc_node();

	/* init the node */
	node->nid = nid;
	node->pid = pid;
	node->key = key;
	node->slot = (bmip_fc_num_slots_used++);

	/* insert the node into the tree */
	bmip_fc_node_ins(node);
}

bmip_fc_node_t * bmip_fc_find_slot(int32_t nid, int32_t pid)
{
	int64_t key = bmip_fc_hash(nid, pid);
	return bmip_fc_node_find(key);
}

int bmip_fc_tree_destroy()
{
	int ret = 0;

	tdestroy(&bmip_fc_conn_tree, bmip_fc_free_node);
	return ret;
}

int bmip_fc_setup_pp_me(bmip_fc_node_t * node)
{
	int ret = 0;

	ret = bmip_ptl_me_attach(node->ni_handle, local_ptl_index, node->ptl_pid, node->mb, node->ib, PTL_RETAIN, PTL_INS_AFTER, &node->me_handle);
	if(ret != PTL_OK)
	{
		goto out;
	}

out:
	return ret;
}

int bmip_fc_setup_pp_md(bmip_fc_node_t * node)
{
	int ret = 0;

	/* setup the md */
	node->md.start = node->data;
	node->md.length = node->length;
	node->md.threshold = PTL_MD_THRESH_INF;
	node->md.options = PTL_MD_OP_PUT | PTL_MD_OP_GET;
	node->md.user_ptr = NULL;
	node->md.eq_handle = node->eq_handle;

	/* attach the md */
	ret = bmip_ptl_md_attach(node->me_handle, node->md, PTL_RETAIN, &node->md_handle);
	if(ret != PTL_OK)
	{
		goto out;
	}

out:
	return ret;
}
