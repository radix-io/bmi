/*
 * Portals BMI method.
 *
 * Copyright (C) 2007-8 Pete Wyckoff <pw@osc.edu>
 * Jason Cope <copej@mcs.anl.gov>, 2009-2010
 *
 * See COPYING in top-level directory.
 */
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>

#if defined(__LIBCATAMOUNT__) || defined(__CRAYXT_COMPUTE_LINUX_TARGET) || defined(__CRAYXT_SERVICE)
/* Cray XT3 and XT4 version, both catamount and compute-node-linux */
#warning "Building for Cray CNL"
#define PTL_IFACE_DEFAULT PTL_IFACE_SS
#include <portals/portals3.h>
#include <sys/utsname.h>
#else
#warning "Building for utcp"
/* TCP version */
#include <portals/portals3.h>
#include <portals/p3nal_utcp.h>  /* sets PTL_IFACE_DEFAULT to UTCP */
#include <portals/p3api/debug.h>
#include <netdb.h>  /* gethostbyname */
#include <arpa/inet.h>  /* inet_ntop */
#endif

#include <assert.h>
#include <sys/signal.h>
#define __PINT_REQPROTO_ENCODE_FUNCS_C  /* include definitions */
#include <src/io/bmi/bmi-method-support.h>   /* bmi_method_ops */
#include <src/io/bmi/bmi-method-callback.h>  /* bmi_method_addr_reg_callback */
#include <src/common/gossip/gossip.h>
#include <src/common/gen-locks/gen-locks.h>  /* gen_mutex_t */
#include <src/common/misc/pvfs2-internal.h>  /* lld */
#include <src/common/id-generator/id-generator.h>

#ifdef __GNUC__
#  define __unused __attribute__((unused))
#else
#  define __unused
#endif

/*
 * Debugging macros.
 */
#if 0                                                  
#define bmip_fprintf(stream,fmt,args...)                \
do{                                                     \
    fprintf(stream, fmt, ##args);                       \
    fflush(stream);                                     \
}while(0)
#else
#define bmip_fprintf(stream,fmt,args...)                \
do{                                                     \
    /* do nothing */                                    \
}while(0)
#endif                                                  

#include "pvfs2-debug.h"
#if 1
#define DEBUG_LEVEL 6
#if 1
#define debug(lvl,fmt,args...) \
    do { \
	if (lvl <= DEBUG_LEVEL) \
	    gossip_debug(GOSSIP_BMI_DEBUG_PORTALS, fmt, ##args); \
    } while (0)
#else
#define debug(lvl,fmt,...) do { } while (0)
#endif
#else
#  define debug(lvl,fmt,...) do { } while (0)
#endif

/* timers */
static double post_recv_time = 0;
static int post_recv_counter = 0;
static double post_send_time = 0;
static int post_send_counter = 0;
static double handle_event_time = 0;
static int handle_event_counter = 0;
static double testcontext_time = 0;
static int testcontext_counter = 0;
static double walloc_time = 0;
static int walloc_counter = 0;
static double wfree_time = 0;
static int wfree_counter = 0;

static inline int bmip_get_time(struct timespec * timeval)
{
    int ret = 0;
    ret = clock_gettime( CLOCK_REALTIME, timeval );

    if(ret != 0)
        fprintf(stderr, "%s:%i timer error\n", __func__, __LINE__);
    return 0;
}

static inline double bmip_elapsed_time(struct timespec * t1, struct timespec * t2)
{
    return ((double) (t2->tv_sec - t1->tv_sec) +
        1.0e-9 * (double) (t2->tv_nsec - t1->tv_nsec) );
}

#define BMIP_MAX_SEGMENT_SIZE 262144
#define BMIP_ALIGN 8 
static inline void * bmip_malloc(size_t len)
{
    return malloc(len);                                                                
} 

static inline void bmip_free(void * ptr)
{                                                                         
    free((ptr));                                                            
}

static void dump_queues(int sig __unused);
/*
 * No global locking.  Portals has its own library-level locking, but
 * we need to keep the BMI polling thread away from the main thread that
 * is doing send/recv.
 */
static gen_mutex_t ni_mutex = GEN_MUTEX_INITIALIZER;
static gen_mutex_t list_mutex = GEN_MUTEX_INITIALIZER;
static gen_mutex_t pma_mutex = GEN_MUTEX_INITIALIZER;
static gen_mutex_t eq_mutex = GEN_MUTEX_INITIALIZER;
static gen_mutex_t op_mutex = GEN_MUTEX_INITIALIZER;

#if 0
#define bmip_gen_mutex_unlock(__lock)                                   \
do {                                                                    \
    bmip_fprintf(stderr, "%s:%i unlock %s\n", __func__, __LINE__, #__lock);  \
    gen_mutex_unlock(__lock);                                           \
    bmip_fprintf(stderr, "%s:%i unlock %s... granted\n", __func__, __LINE__, #__lock);  \
} while(0)

#define bmip_gen_mutex_trylock(__retval, __lock)                        \
do {                                                                    \
    bmip_fprintf(stderr, "%s:%i trylock %s\n", __func__, __LINE__, #__lock); \
    __retval = gen_mutex_trylock(__lock);                               \
    if(__retval == -1 && errno == -EBUSY)                               \
    {                                                                   \
        bmip_fprintf(stderr, "%s:%i trylock %s... not granted\n", __func__, __LINE__, #__lock);  \
    }                                                                   \
    else                                                                \
    {                                                                   \
        bmip_fprintf(stderr, "%s:%i trylock %s... granted\n", __func__, __LINE__, #__lock);  \
    }                                                                   \
} while(0)

#define bmip_gen_mutex_lock(__lock)                                     \
do {                                                                    \
    bmip_fprintf(stderr, "%s:%i lock %s\n", __func__, __LINE__, #__lock);    \
    gen_mutex_lock(__lock);                                             \
    bmip_fprintf(stderr, "%s:%i lock %s... granted\n", __func__, __LINE__, #__lock);    \
} while(0)

#else

#define bmip_gen_mutex_unlock(__lock) gen_mutex_unlock(__lock)
#define bmip_gen_mutex_lock(__lock) gen_mutex_lock(__lock)

#endif

/*
 * Handle given by upper layer, which must be handed back to create
 * method_addrs.
 */
static int bmi_portals_method_id;

/*
 * Various static ptls objects.  One per instance of the code.
 */
static ptl_handle_ni_t ni = PTL_INVALID_HANDLE;
static ptl_handle_me_t mark_me = PTL_INVALID_HANDLE;
static ptl_handle_me_t zero_me = PTL_INVALID_HANDLE;
static ptl_handle_md_t zero_md;
static ptl_handle_eq_t eq = PTL_INVALID_HANDLE;
static int ni_init_dup;  /* did runtime open the nic for us? */

/*
 * Server listens at this well-known portal for unexpected messages.  Clients
 * will let portals pick a portal for them.  There tend not to be too many of
 * these:  utcp has 8, so pick a small number.
 *
 * This is also used for clients.  If they used MEAttachAny, then
 * we'd have to remember the ptl_index from a sendunexpected over to the
 * answering send call in the server.  No need for connection state just
 * for that.
 *
 * Cray has a bunch reserved.  SNLers suggest using ARMCI's.
 */
static const ptl_pt_index_t ptl_index = 37;

/*
 * Handy const.  Cray version needs padding, though, so initialize this
 * elsewhere.
 */
static ptl_process_id_t any_pid;

/*
 * Cray does not have these, but TCP does.
 */
#ifndef HAVE_PTLERRORSTR
static const char *PtlErrorStr(unsigned int ptl_errno)
{
    return ptl_err_str[ptl_errno];
}
#endif

#ifndef HAVE_PTLEVENTKINDSTR
static const char *PtlEventKindStr(ptl_event_kind_t ev_kind)
{
    extern const char *ptl_event_str[];

    return ptl_event_str[ev_kind];
}
#endif

/*
 * Match bits.  The lower 32 bits always carry the bmi_tag.  If this bit
 * in the top is set, it is an unexpected message.  The secondmost top bit is
 * used when posting a _send_, strangely enough.  If the send is too long,
 * and the receiver has not preposted, later the receiver will issue a Get
 * to us for the data.  That get will use the second set of match bits.
 *
 * The rest of the 30 top bits are used to encode a sequence number per
 * peer.  As BMI can post multiple sends with the same tag, we have to
 * be careful that if send #2 for a given tag goes to the zero_md, that
 * when he does the get, he grabs from buffer #2, not buffer #1 because
 * the sender was too slow in unlinking it.
 */
static const uint64_t match_bits_unexpected = 1ULL << 63;  /* 8... */
static const uint64_t match_bits_long_send = 1ULL << 62;   /* 4... */
static const uint64_t match_bits_reset_seq = 1ULL << 61;   /* 2... */ /* 1 = reset seq counter */
static int reset_seqno_flag = 1;
static const uint32_t match_bits_seqno_max = 1UL << 20;
static const uint32_t match_bits_subseqno_max = 1UL << 9;
static const int      match_bits_seqno_shift = 32;
static const int      match_bits_subseqno_shift = 20;

static uint64_t mb_from_tag_and_seqno(uint32_t tag, uint32_t seqno)
{
    uint64_t mb;

    mb = seqno;
    mb <<= match_bits_seqno_shift;
    mb |= tag;
    /* caller may set the long send bit too */
    return mb;
}

static uint64_t mb_from_tag_and_seqno_and_subseqno(uint32_t tag, uint32_t seqno, uint32_t subseqno)
{
    uint64_t mb;

    /* set the subsequence */
    //mb = subseqno;
    //mb <<= match_bits_subseqno_shift;

    /* set the sequence */
    mb |= seqno;
    mb <<= match_bits_seqno_shift;

    /* set the tag */
    mb |= tag;

    /* caller may set the long send bit too */
    return mb;
}

/*
 * Buffer for incoming unexpected send messages.  Only the server needs
 * a list of these.  Each message can be no more than 8k.  The total memory
 * devoted to these on the server is 256k.  Not each unexpected message
 * will take the full 8k, depending on the size of the request.
 *
 * Arriving data is copied out of these as quickly as possible.  No refcnt
 * as in the nonprepost case below, where data sits around in those buffers.
 */
#define UNEXPECTED_MESSAGE_SIZE (8 << 10)
#define UNEXPECTED_QUEUE_SIZE   (4 << 20)
#define UNEXPECTED_NUM_MD 2
#define UNEXPECTED_SIZE_PER_MD  (UNEXPECTED_QUEUE_SIZE/UNEXPECTED_NUM_MD)

#define UNEXPECTED_MD_INDEX_OFFSET (1)
#define NONPREPOST_MD_INDEX_OFFSET (UNEXPECTED_NUM_MD + 1)

static char *unexpected_buf = NULL;
/* poor-man's circular buffer */
static ptl_handle_me_t unexpected_me[UNEXPECTED_NUM_MD];
static ptl_handle_md_t unexpected_md[UNEXPECTED_NUM_MD];
static int unexpected_need_repost[UNEXPECTED_NUM_MD];
static int unexpected_need_repost_sum;
static int unexpected_is_posted[UNEXPECTED_NUM_MD];

/*
 * This scheme relies on the zero page being unused, i.e. addrsesses
 * from 0 up to 4k or so.
 */
static int unexpected_md_index(void *user_ptr)
{
    int i;
    uintptr_t d = (uintptr_t) user_ptr;

    if (d >= UNEXPECTED_MD_INDEX_OFFSET &&
        d < UNEXPECTED_MD_INDEX_OFFSET + UNEXPECTED_NUM_MD)
	return d - UNEXPECTED_MD_INDEX_OFFSET;
    else
	return -1;
}

/*
 * More buffers for non-preposted receive handling.  While the unexpected
 * ones above are for explicit API needs of BMI, these are just to work
 * around the lack of a requirement to pre-post buffers in BMI.  Portals
 * would otherwise drop messages when there is no matching receive.  Instead
 * we grab them (or parts of them) with these buffers.
 *
 * Only the first 8k of each non-preposted message is saved, but we leave
 * room for up to 8M of these.  If the total message size is larger, the
 * receiver will do a Get to fetch the rest.  Note that each non-preposted
 * message causes a struct bmip_work to be malloced too.
 *
 * The nonprepost_refcnt[] variable handles what happens when the MD is
 * unlinked.  As nonprepost messages arrive, each new rq adds 1 to the
 * refcnt.  As receives are posted and consume rqs, the refcnt drops.
 * If an UNLINK event happens, is_posted goes zero.  If an rq consumption
 * drops the refcnt to zero, and it is not posted, it is reinitialized
 * and reposted.  Whew.
 */
#define NONPREPOST_MESSAGE_SIZE (8 << 10)
#define NONPREPOST_QUEUE_SIZE (8 << 20)
#define NONPREPOST_NUM_MD 2
#define NONPREPOST_SIZE_PER_MD  (NONPREPOST_QUEUE_SIZE/NONPREPOST_NUM_MD)

static char *nonprepost_buf = NULL;
/* poor-man's circular buffer */
static ptl_handle_me_t nonprepost_me[NONPREPOST_NUM_MD];
static ptl_handle_md_t nonprepost_md[NONPREPOST_NUM_MD];
static int nonprepost_need_repost[NONPREPOST_NUM_MD];
static int nonprepost_need_repost_sum;
static int nonprepost_is_posted[NONPREPOST_NUM_MD];
static int nonprepost_refcnt[NONPREPOST_NUM_MD];

static int nonprepost_md_index(void *user_ptr)
{
    int i;
    uintptr_t d = (uintptr_t) user_ptr;

    if (d >= NONPREPOST_MD_INDEX_OFFSET &&
        d < NONPREPOST_MD_INDEX_OFFSET + NONPREPOST_NUM_MD)
    {
	    return d - NONPREPOST_MD_INDEX_OFFSET;
    }
    else
    {
	    return -1;
    }
}

/*
 * "private data" part of method_addr.  These describe peers, so
 * have the portal and pid of the remote side.  We need to keep a list
 * to be able to find a pma from a given pid, even though these are
 * already kept in a list up in BMI.  Some day fix that interface.
 */
struct bmip_method_addr {
    struct qlist_head list;
    char *hostname;  /* given by user, converted to a nid by us */
    char *peername;  /* for rev_lookup */
    ptl_process_id_t pid;  /* this is a struct with u32 nid + u32 pid */
    uint32_t seqno_out;  /* each send has a separate sequence number */
    uint32_t seqno_in;
};
static QLIST_HEAD(pma_list);

/*
 * Work item queue states document the lifetime of a send or receive.
 */
enum work_state {
    SQ_WAITING_ACK,
    SQ_WAITING_GET,
    SQ_WAITING_USER_TEST,
    SQ_CANCELLED,
    RQ_WAITING_INCOMING,
    RQ_WAITING_GET,
    RQ_WAITING_PUT_END,
    RQ_WAITING_PUT_END_GET_READY,
    RQ_WAITING_USER_TEST,
    RQ_WAITING_USER_POST,
    RQ_LEN_ERROR,
    RQ_CANCELLED,
};

static const char *state_name(enum work_state state)
{
    switch (state) {
    case SQ_WAITING_ACK:
	return "SQ_WAITING_ACK";
    case SQ_WAITING_GET:
	return "SQ_WAITING_GET";
    case SQ_WAITING_USER_TEST:
	return "SQ_WAITING_USER_TEST";
    case SQ_CANCELLED:
	return "SQ_CANCELLED";
    case RQ_WAITING_INCOMING:
	return "RQ_WAITING_INCOMING";
    case RQ_WAITING_GET:
	return "RQ_WAITING_GET";
    case RQ_WAITING_PUT_END:
	return "RQ_WAITING_PUT_END";
    case RQ_WAITING_PUT_END_GET_READY:
	return "RQ_WAITING_PUT_END_GET_READY";
    case RQ_WAITING_USER_TEST:
	return "RQ_WAITING_USER_TEST";
    case RQ_WAITING_USER_POST:
	return "RQ_WAITING_USER_POST";
    case RQ_LEN_ERROR:
	return "RQ_LEN_ERROR";
    case RQ_CANCELLED:
	return "RQ_CANCELLED";
    }
    return "(unknown state)";
}

struct bmip_master_mp
{
    struct qlist_head list;
    bmi_op_id_t master_id;
    uint32_t count;
    bmi_size_t accum_size;
};

#define BMIP_WORK_MULTI_MASTER_ALLOC(_p) \
do{ \
    (_p) = (struct bmip_master_mp *)calloc(1, sizeof(struct bmip_master_mp)); \
    (_p)->accum_size = 0;\
}while(0)

#define BMIP_WORK_MULTI_MASTER_FREE(_p) \
do{ \
    free((_p)); \
}while(0)

struct bmip_multi_work {
    struct qlist_head list;
    bmi_op_id_t id;
    bmi_op_id_t master_id;
    bmi_msg_tag_t tag;
    uint32_t index;
    uint32_t total;
    void * user_ptr;
    bmi_size_t accum_bytes;
};

#define BMIP_WORK_MULTI_ALLOC(_p) \
do{ \
    (_p) = (struct bmip_multi_work *)calloc(1, sizeof(struct bmip_multi_work)); \
    (_p)->user_ptr = NULL;\
}while(0)

#define BMIP_WORK_MULTI_FREE(_p) \
do{ \
    free((_p)); \
}while(0)

/*
 * Common structure for both send and recv outstanding work items.  There
 * is a queue of these for each send and recv that are frequently searched.
 */
struct bmip_work {
    struct qlist_head list;
    int type;               /* BMI_SEND or BMI_RECV */
    enum work_state state;  /* send or receive state */
    struct method_op mop;   /* so BMI can use ids to find these */

    /* mdesc data */
    void * mdesc_start;
    ptl_size_t mdesc_length; 
    unsigned int mdesc_options;

    bmi_size_t tot_len;     /* size posted for send or recv */
    bmi_size_t actual_len;  /* recv: possibly shorter than posted */

    bmi_msg_tag_t bmi_tag;  /* recv: unexpected or nonpp tag that arrived */
    uint64_t match_bits;    /* recv: full match bits, including seqno */

    int is_unexpected;      /* send: if user posted this as unexpected */

    ptl_handle_me_t me;     /* recv: prepost match list entry */
			    /* send: send me for possible get */
    ptl_handle_md_t md;     /* recv: prepost or get destination, to cancel */
			    /* send: send md for possible get */
    int saw_send_end_and_ack; /* send: make sure both states before unlink */

    int manual_md_unlink;
    int manual_me_unlink;
    /* non-preposted receive, keep ref to a nonpp static buffer */
    const void *nonpp_buf;   /* pointer to nonpp buffer in MD */
    int nonpp_md;            /* index into nonprepost_md[] */

    /* unexpected receive, unex_buf is malloced to hold the data */
    void *unex_buf;

    /* user buffers */
    int user_numbufs;
    void * user_buffers; 
    bmi_size_t * user_sizes;
    bmi_size_t user_total_size;
    char * alignbm;
};

static void build_mdesc(struct bmip_work *w, ptl_md_t *mdesc, int numbufs,
            void *const *buffers, const bmi_size_t *sizes);

static void build_mdesc_work(struct bmip_work *w, ptl_md_t *mdesc);

static void cleanup_mdesc(struct bmip_work *w);

#define BMIP_WORK_ALLOC(_p)                                                 \
do {                                                                        \
    (_p) = (ptl_md_iovec_t *)calloc(1, sizeof(struct bmip_work));           \
    (_p)->md = -1; \
    (_p)->me = -1; \
} while(0)

#define BMIP_WORK_FREE(_p)                                  \
do {                                                        \
    if((_p))                                                \
    {                                                       \
        if((_p)->manual_me_unlink && (_p)->me != PTL_INVALID_HANDLE)       \
        {                 \
            bmip_fprintf(stderr, "%s:%i md = %i me = %i\n", __func__, __LINE__, (_p)->md, (_p)->me); \
            int lret = PtlMEUnlink((_p)->me);                        \
            if(lret)                                       \
            {                                               \
                bmip_fprintf(stderr, "%s:%i PtlMEUnlink failed, err = %s\n", __func__, __LINE__, PtlErrorStr(lret)); \
            }                                               \
        }                                                   \
        if((_p)->user_sizes)      \
        {                                                   \
            bmip_free((_p)->user_sizes);              \
        }                                                   \
        if((_p)->user_buffers && (_p)->user_numbufs > 1)    \
        {                                                   \
            bmip_free((_p)->user_buffers);            \
        }                                                   \
        cleanup_mdesc((_p));                                \
        (_p)->user_sizes = NULL;                            \
        (_p)->user_buffers = NULL;                          \
        (_p)->user_numbufs = 0;                             \
        bmip_free(_p);                                    \
        (_p) = NULL;                                        \
    }                                                       \
} while(0)                                                  

/*
 * All operations are on exactly one list.  Even queue items that cannot
 * be reached except from BMI lookup are on a list.  The lists are pretty
 * specific.
 *
 * q_send_waiting_ack - sent the message, waiting for ack
 * q_send_waiting_get - sent the message, ack says truncated, wait his get
 * q_recv_waiting_incoming - posted recv, waiting for data to arrive
 * q_recv_waiting_get - he sent before we recvd, now we sent get
 * q_recv_nonprepost - data arrived before recv was posted
 * q_unexpected_done - unexpected message arrived, waiting testunexpected
 * q_done - send or recv completed, waiting caller to test
 */
/* lists that are never shared, or even looked at, but keep code prettier so
 * we don't have to see if we need to qdel before qadd */
static QLIST_HEAD(q_send_waiting_ack);
static QLIST_HEAD(q_send_waiting_get);
static QLIST_HEAD(q_recv_waiting_incoming);
static QLIST_HEAD(q_recv_waiting_get);
static QLIST_HEAD(q_recv_waiting_put_end);
static QLIST_HEAD(q_recv_nonprepost_start);
static QLIST_HEAD(q_recv_nonprepost);

/* real lists that need locking between event handler and test/post */
static QLIST_HEAD(q_unexpected_done);
static QLIST_HEAD(q_done);
static QLIST_HEAD(q_mp_done);
static QLIST_HEAD(q_master_mp);

static struct bmi_method_addr *addr_from_nidpid(ptl_process_id_t pid);
static void unexpected_repost(int which);
static int nonprepost_init(void);
static int nonprepost_fini(void);
static int unexpected_fini(void);
static void nonprepost_repost(int which);
static const char *bmip_rev_lookup(struct bmi_method_addr *addr);

static void copy_mop(struct bmip_work * o, struct bmip_work * n)
{
    n->mop.addr = o->mop.addr;
    n->mop.method_data = o->mop.method_data;
    n->mop.user_ptr = o->mop.user_ptr;
    n->mop.context_id = o->mop.context_id;
    n->mop.op_id = o->mop.op_id;
}

static void rebuild_mdesc(struct bmip_work * w, ptl_md_t * mdesc)
{
    //mdesc->threshold = 1;
    mdesc->threshold = PTL_MD_THRESH_INF;
    mdesc->options = 0;  /* PTL_MD_EVENT_START_DISABLE; */
    mdesc->eq_handle = eq;
    mdesc->user_ptr = w;

    mdesc->length = w->mdesc_length;
    mdesc->start = w->mdesc_start;
    mdesc->options = w->mdesc_options;

    fprintf(stderr, "%s:%i w = %p\n", __func__, __LINE__, w);
}

/*----------------------------------------------------------------------------
 * Test
 */

/*
 * Deal with an event, advancing the relevant state machines.
 */
static int handle_event(ptl_event_t *ev)
{
    struct bmip_work *sq, *sqn, *rq, *rqn;
    int which, ret = 0;

    if (ev->ni_fail_type != 0) 
    {
	    bmip_fprintf(stderr, "%s: ni err %d\n", __func__, ev->ni_fail_type);
	    return -EIO;
    }

    bmip_fprintf(stderr, "%s: event type %s, nid %i, pid %i, match_bits %llx, rlength %i, mlength %i, offset %i\n", __func__, PtlEventKindStr(ev->type), ev->initiator.nid, ev->initiator.pid, ev->match_bits, ev->rlength, ev->mlength, ev->offset);

    switch (ev->type)
    {
        /* put operations event types */
        /* put initiator event */
        case PTL_EVENT_SEND_START:
        {
            sq = ev->md.user_ptr;

            /* this is triggered by a get() operation ... check if the operation type is for a recv */
            if(sq->type == BMI_RECV)
            {
                break;
            }
            /* do nothing here */
	        break;
        }
        /* put initiator event */
        case PTL_EVENT_SEND_END:
	    {
            sq = ev->md.user_ptr;

            /* this is triggered by a get() operation ... check if the operation type is for a recv */
            if(sq->type == BMI_RECV)
            {
                break;
            }
           
            /* if this is not an unexpected message and the SEND_END and ACK both occured */ 
            if(!sq->is_unexpected && ++sq->saw_send_end_and_ack == 2)
            {
                /* unlink the mesg */
                ret = PtlMEUnlink(sq->me);
                if(ret)
                {
                    fprintf(stderr, "%s:%i : PtlMEUnlink error, %s\n", __func__, __LINE__, PtlErrorStr(ret));
                }
                sq->manual_me_unlink = 0;
            } 
            break;
        }

        /* put target event */
        case PTL_EVENT_PUT_START:
	    {
            which = unexpected_md_index(ev->md.user_ptr);
            /* if this was an unexpected message */
            if(which >= 0)
            {
                /* do nothing */
            }
           
            which = nonprepost_md_index(ev->md.user_ptr);
            /* if this was a nonpreposted message */
            if(which >= 0 || ev->md_handle == zero_md)
            {
                int found = 0;
                //bmip_gen_mutex_lock(&list_mutex);
                qlist_for_each_entry_safe(rq, rqn, &q_recv_nonprepost_start, list)
                {
                    /* if the buffer request matches, mark as found and remove it from the list */
                    if(rq->mop.addr == addr_from_nidpid(ev->initiator) && rq->bmi_tag == (ev->match_bits & 0xffffffffULL) && rq->match_bits == ev->match_bits)
                    {
                        found = 1;
                    }
                }
                //bmip_gen_mutex_unlock(&list_mutex);

                /* if this was already on the list... exit */
                if(found)
                {
                    break;
                }

                /* check the wait incomming list */
                found = 0;
                //bmip_gen_mutex_lock(&list_mutex);
                qlist_for_each_entry_safe(rq, rqn, &q_recv_waiting_incoming, list)
                {
                    if(rq->mop.addr == addr_from_nidpid(ev->initiator) && rq->bmi_tag == (ev->match_bits & 0xffffffffULL) && rq->match_bits == ev->match_bits)
                    {
                        found = 1;
                        break;
                    }
                }

                /* if we found this message on the waiting list, don't reallocate it */
                if(found)
                {
                   qlist_del(&(rq->list)); 
                }
                /* else make a new request */
                else
                {
                    BMIP_WORK_ALLOC(rq);

                    rq->type = BMI_RECV;
                    rq->state = RQ_WAITING_PUT_END;
                    rq->tot_len = ev->rlength;
                    rq->actual_len = 0;
                    rq->bmi_tag = ev->match_bits & 0xffffffffULL;
                    rq->match_bits = ev->match_bits;
                    rq->mop.addr = addr_from_nidpid(ev->initiator);

                    /* init the user buffers */
                    rq->user_buffers = NULL;
                    rq->user_sizes = NULL;
                    rq->user_total_size = 0;
                    rq->user_numbufs = 0;
                }

                /* add this new request to the the recv'd nonprepost list */
                qlist_add_tail(&rq->list, &q_recv_nonprepost_start);
                //bmip_gen_mutex_unlock(&list_mutex);

                break;
            }
            /* this was a preposted message */
            else
            {
                /* do nothing */
            }
            break;
        }
        /* put target event */
        case PTL_EVENT_PUT_END:
	    {
            which = unexpected_md_index(ev->md.user_ptr);
            /* if this was an unexpected message */
            if(which >= 0)
            {

                /* allocate the work object for the unexpected message */
                BMIP_WORK_ALLOC(rq);
                if(!rq)
                {
                    bmip_fprintf(stderr, "%s : could not allocate unexpected message work object\n", __func__);
                }

                /* make sure that the message length is not large than the unexpected message size limit */
                if(ev->mlength > UNEXPECTED_MESSAGE_SIZE)
                {
                    exit(1);
                }

                /* 
                 * init the work request for the unexpected message
                 * 1) mark this as a recieved message
                 * 2) allocate the buffer for the messagee
                 * 3) set the message length
                 * 4) extract the message tag from the match bits
                 * 5) extract the class from the match bits
                 * 6) setup the mop addr from the portal event addr
                 * 7) copy the event memory into the unexpected message buffer
                 */
                rq->type = BMI_RECV;
                posix_memalign(&(rq->unex_buf), BMIP_ALIGN, ev->mlength);
                if(!rq->unex_buf)
                {
                    bmip_fprintf(stderr, "%s : could not allocate the unexpected message buffer\n", __func__);
                    BMIP_WORK_FREE(rq);
                    break;
                }
                rq->actual_len = ev->mlength;
                rq->bmi_tag = ev->match_bits & 0xffffffffULL;
                rq->match_bits = ev->match_bits;

                rq->mop.addr = addr_from_nidpid(ev->initiator);

                /* if the reset flag was set... */
                if((rq->match_bits & match_bits_reset_seq) != 0)
                {
                    /* reset the in and out sequence numbers */
                    struct bmip_method_addr * upma = rq->mop.addr->method_data;
                    upma->seqno_out = 0; 
                    upma->seqno_in = 0; 
                }

                memcpy(rq->unex_buf, (char *)ev->md.start + ev->offset, ev->mlength);

                /* init the user buffers */
                rq->user_buffers = NULL;
                rq->user_sizes = NULL;
                rq->user_total_size = -1;
                rq->user_numbufs = 0;

                /* add the unexpected message to the done list */
                bmip_gen_mutex_lock(&list_mutex);
                qlist_add_tail(&rq->list, &q_unexpected_done);
                bmip_gen_mutex_unlock(&list_mutex);

                /* repost the unexpected message if unexpected messages could still be stored */
                if(UNEXPECTED_SIZE_PER_MD - ev->offset < 100 * UNEXPECTED_MESSAGE_SIZE)
                {
                    /* if the current message slot is not set to repost, set it to repost */
                    if(unexpected_need_repost[which] == 0)
                    {
                        unexpected_need_repost[which] = 1;
                        ++unexpected_need_repost_sum;
                    }
                }

                /* unpost some of the slots if they are free */
                if(unexpected_need_repost_sum)
                {
                    /* for each unexpected message slot */
                    for(which = 0 ; which < UNEXPECTED_NUM_MD ; which++)
                    {   
                        /* if message needs to be reposted */
                        if(unexpected_need_repost[which])
                        {
                            /* repost the message slot */
                            unexpected_repost(which);
                        }
                    }
                }

                /* unexpected message done */
                break;
            }

            which = nonprepost_md_index(ev->md.user_ptr);
            /* if this was a nonpreposted message */
            if(which >= 0 || ev->md_handle == zero_md)
            {
                /* check if this transfer is on the waiting incoming list */
                int found = 0;
                //bmip_gen_mutex_lock(&list_mutex);
                qlist_for_each_entry_safe(rq, rqn, &q_recv_waiting_incoming, list)
                {
                    /* if the buffer request matches, mark as found and remove it from the list */
                    if(rq->mop.addr == addr_from_nidpid(ev->initiator) && rq->bmi_tag == (ev->match_bits & 0xffffffffULL) && rq->match_bits == ev->match_bits)
                    {
                        rq->state = RQ_WAITING_USER_TEST;

                        /* the actual length exceeds the total length of the buffer... error */
                        if(rq->actual_len > rq->tot_len)
                        {
                            rq->state = RQ_LEN_ERROR;
                        }
                        found = 1;

                        break;
                    }
                }
                if(found)
                {
                    qlist_del(&rq->list);
                    bmip_gen_mutex_lock(&list_mutex);
                    qlist_add_tail(&rq->list, &q_done);
                    bmip_gen_mutex_unlock(&list_mutex);
                    break;
                }
                //bmip_gen_mutex_unlock(&list_mutex);
 
                /* get the recv request from the nonprepost start list */
                found = 0;
                //bmip_gen_mutex_lock(&list_mutex);
                qlist_for_each_entry_safe(rq, rqn, &q_recv_nonprepost_start, list)
                {
                    /* if the buffer request matches, mark as found and remove it from the list */
                    if (addr_from_nidpid(ev->initiator) == rq->mop.addr && rq->bmi_tag == (ev->match_bits & 0xffffffffULL) && rq->match_bits == ev->match_bits)
                    {
                        found = 1;
                        break;
                    }
                }
                if(found)
                {
                    qlist_del(&rq->list);
                }
                //bmip_gen_mutex_unlock(&list_mutex);

                /* if this was a nonprepost operation */
                if(found)
                {
                    /* if the handle was the zero md, we dropped the buffer. */
                    if(ev->md_handle == zero_md)
                    {
                        /* if this was a long, nonpreposted message and we dropped it. Make a new
                         * request setup for a long recv.
                         */
                        if(ev->rlength > NONPREPOST_MESSAGE_SIZE)
                        {
                            /* recv not yet posted... add it to the nonprepost list */
                            if(rq->state == RQ_WAITING_PUT_END)
                            {
                                /* properly inidcate that this is a long messages */
                                rq->match_bits = ev->match_bits | match_bits_long_send;
                                rq->tot_len = ev->rlength;
                                rq->actual_len = 0; 

                                /* add it to the nonprepost list */
                                //bmip_gen_mutex_lock(&list_mutex);
                                qlist_add_tail(&rq->list, &q_recv_nonprepost);
                                //bmip_gen_mutex_unlock(&list_mutex);
                            }
                            /* recv was already posted, issue Get here */
                            else if(rq->state == RQ_WAITING_PUT_END_GET_READY)
                            {
                                ptl_md_t mdesc;
                                rq->match_bits |= match_bits_long_send;
                                rq->tot_len = ev->rlength;
                                rq->actual_len = 0;


                                /* the md is already init */
                                if(rq->md != -1)
                                {
                                    ret = PtlMDUpdate(rq->md, &mdesc, NULL, eq);
                                    if(ret)
                                    {
                                        bmip_fprintf(stderr, "%s %i: PtlMDUpdate() failed: %s\n", __func__, __LINE__, PtlErrorStr(ret));
                                    }

                                    build_mdesc_work(rq, &mdesc);
                                    mdesc.threshold = PTL_MD_THRESH_INF; /* send and reply */
                                    mdesc.options |= PTL_MD_OP_GET | PTL_MD_OP_PUT;

                                    ret = PtlMDUpdate(rq->md, NULL, &mdesc, eq);
                                    if(ret)
                                    {
                                        bmip_fprintf(stderr, "%s %i: PtlMDUpdate() failed: %s\n", __func__, __LINE__, PtlErrorStr(ret));
                                    }
   
                                    ret = PtlGet(rq->md, ev->initiator, ptl_index, 0, rq->match_bits, 0);
                                    if (ret)
                                    {
                                        bmip_fprintf(stderr, "%s: PtlGet() failed: %s\n", __func__, PtlErrorStr(ret));
                                    }

                                    rq->state = RQ_WAITING_INCOMING;
                                    //bmip_gen_mutex_lock(&list_mutex);
                                    qlist_add_tail(&rq->list, &q_recv_waiting_incoming);
                                    //bmip_gen_mutex_unlock(&list_mutex);
                                }
                                /* we need to init the md and the me... do we have enough info here? no... */
                                else
                                {
                                    build_mdesc_work(rq, &mdesc);
                                    mdesc.threshold = PTL_MD_THRESH_INF; /* send and reply */
                                    mdesc.options |= PTL_MD_OP_GET | PTL_MD_OP_PUT;

                                    /* create new match entry for this recv */
                                    ret = PtlMEInsert(mark_me, ev->initiator, rq->match_bits, 0, PTL_UNLINK,
                                        PTL_INS_BEFORE, &rq->me);
                                    if (ret)
                                    {
                                        fprintf(stderr, "%s: PtlMEInsert: %s\n", __func__, PtlErrorStr(ret));
                                    }
                                    rq->manual_me_unlink = 1;

                                    /* create a new mem desciptor and attach the match entry to it */
                                    ret = PtlMDAttach(rq->me, mdesc, PTL_UNLINK, &rq->md);
                                    if (ret)
                                    {
                                        bmip_fprintf(stderr, "%s: PtlMDAttach: %s\n", __func__, PtlErrorStr(ret));
                                    }
                                    rq->manual_md_unlink = 1;

                                    ret = PtlGet(rq->md, ev->initiator, ptl_index, 0, rq->match_bits, 0);
                                    if (ret)
                                    {
                                        bmip_fprintf(stderr, "%s: PtlGet() failed: %s\n", __func__, PtlErrorStr(ret));
                                    }

                                    rq->state = RQ_WAITING_INCOMING;
                                    //bmip_gen_mutex_lock(&list_mutex);
                                    qlist_add_tail(&rq->list, &q_recv_waiting_incoming);
                                    //bmip_gen_mutex_unlock(&list_mutex);
                                }
                            }
                            /* unknown state encountered */
                            else
                            {
                                bmip_fprintf(stderr, "%s : unknown state, expected RQ_WAITING_PUT_END or RQ_WAITING_PUT_END_GET_READY, state == %s\n", __func__, state_name(rq->state));
                            }
                        }
                        /* else, this was a short, preposted message... the circular buffer is not posted! */
                        else
                        {
                            bmip_fprintf(stderr, "%s : ERROR! short, preposted message dropped\n", __func__);
                            exit(0);
                        }
                        rq->nonpp_buf = NULL;

                        break;
                    }
                    /* otherwise, we have the buffer... add it to this work request */
                    else if(ev->md.user_ptr != NULL)
                    {
                        rq->type = BMI_RECV;
                        rq->actual_len = ev->mlength;
                        rq->bmi_tag = ev->match_bits & 0xffffffffULL;
                        rq->match_bits = ev->match_bits;
                        rq->mop.addr = addr_from_nidpid(ev->initiator);

                        /* repost the noprepost message if nonprepost messages could still be stored */
                        if(NONPREPOST_SIZE_PER_MD - ev->offset < 10 * NONPREPOST_MESSAGE_SIZE)
                        {
                            /* if the current message slot is not set to repost, set it to repost */
                            if(nonprepost_need_repost[which] == 0)
                            {
                                nonprepost_need_repost[which] = 1;
                                ++nonprepost_need_repost_sum;
                            }
                        }

                        /* add the request to the done queue */
                        bmip_gen_mutex_lock(&list_mutex);
                        qlist_del(&rq->list);
                        bmip_gen_mutex_unlock(&list_mutex);

                        /* if we already executed post_recv / match_nonprepost_recv */
                        if(rq->state == RQ_WAITING_PUT_END_GET_READY)
                        {
                            /* copy the data into the user buffers */
                            if(rq->user_buffers)
                            {
                                /* copy the memory */

                                /* single buffer, just copy it */
                                if(rq->user_numbufs < 2)
                                {
                                    memcpy((char *)rq->user_buffers, (char *)ev->md.start + ev->offset, ev->mlength);
                                }
                                /* multiple buffers, track the offset and copy each buffer */
                                else
                                {
                                    /* TODO verify that ev->mlength and the total exepected list size are equal */

                                    int nindex = 0;
                                    size_t curofs = 0;
                                    for(nindex = 0 ; nindex < rq->user_numbufs ; nindex++)
                                    {
                                        memcpy(((char **)rq->user_buffers)[nindex], (char *)ev->md.start + ev->offset + curofs, rq->user_sizes[nindex]);
                                        curofs += rq->user_sizes[nindex];
                                    }
                                }

                                /* unpost some of the slots if they are free */
                                if(nonprepost_need_repost_sum)
                                {
                                    /* for each nonprepost message slot */
                                    int npi = 0;
                                    for(npi = 0 ; npi < NONPREPOST_NUM_MD ; npi++)
                                    {
                                        /* if message needs to be reposted */
                                        if(nonprepost_need_repost[npi] && nonprepost_refcnt[rq->nonpp_md] == 0)
                                        {
                                            /* repost the message slot */
                                            nonprepost_repost(npi);
                                       }
                                    }
                                }
                            }
                            rq->state = RQ_WAITING_USER_TEST;
                            bmip_gen_mutex_lock(&list_mutex);
                            qlist_add_tail(&rq->list, &q_done);
                            bmip_gen_mutex_unlock(&list_mutex);
                        }
                        /* else, wait for the recv / match_nonprepost */
                        else
                        {
                            rq->nonpp_buf = (char *)ev->md.start + ev->offset;
                            rq->nonpp_md = which;
                            ++nonprepost_refcnt[rq->nonpp_md];

                            rq->state = RQ_WAITING_USER_POST;
                            //bmip_gen_mutex_lock(&list_mutex);
                            qlist_add_tail(&rq->list, &q_recv_nonprepost);
                            //bmip_gen_mutex_unlock(&list_mutex);
                        }

                        break;
                    }
                    /* else, user_ptr is NULL */
                    else
                    {
                        bmip_fprintf(stderr, "%s : ERROR user_ptr is NULL\n", __func__);
                    }

                    /* add this new request to the the recv'd nonprepost list */
                    //bmip_gen_mutex_lock(&list_mutex);
                    qlist_add_tail(&rq->list, &q_recv_nonprepost);
                    //bmip_gen_mutex_unlock(&list_mutex);
                }
                break;
            }
            /* this was a preposted message */
            else
            {
                rq = ev->md.user_ptr;
                rq->actual_len = ev->rlength;
                rq->state = RQ_WAITING_USER_TEST;
                
                /* the actual length exceeds the total length of the buffer... error*/
                if(rq->actual_len > rq->tot_len)
                {
                    rq->state = RQ_LEN_ERROR;
                }

                /* remove the request from its current list and add it to the done list */
                bmip_gen_mutex_lock(&list_mutex);
                qlist_del(&rq->list);
                qlist_add_tail(&rq->list, &q_done);
                bmip_gen_mutex_unlock(&list_mutex);
            
                break;
            } 
            break;
        }
        /* put initiator event */
        case PTL_EVENT_ACK:
        {
            sq = ev->md.user_ptr;

            /* if the message length was greater than 0 */
            if(ev->mlength > 0 && ev->mlength == ev->rlength)
            {
                /* if this is not an unexpected message and the SEND_END and ACK both occured */ 
                if(!sq->is_unexpected && ++sq->saw_send_end_and_ack == 2)
                {
                    /* unlink the mesg */
                    ret = PtlMEUnlink(sq->me);
                    if(ret)
                    {
                        fprintf(stderr, "%s:%i : PtlMEUnlink error, %s\n", __func__, __LINE__, PtlErrorStr(ret));
                    }
                    sq->manual_me_unlink = 0;
                }
           
                /* operation is waiting for a user test */ 
                sq->state = SQ_WAITING_USER_TEST;
                sq->actual_len = ev->mlength;

                /* update the lists... remove the message from its current list and add it to the done list */
                bmip_gen_mutex_lock(&list_mutex);
                qlist_del(&sq->list);
                qlist_add_tail(&sq->list, &q_done);
                bmip_gen_mutex_unlock(&list_mutex);
            }
            /* else, the whole message was not sent. queue it and wait for the reciever to be ready and initiate the get */
            else
            {
                /* operation is waiting for a user test */
                sq->state = SQ_WAITING_GET;
                sq->match_bits |= match_bits_long_send;
                sq->actual_len = 0;

                /* update the lists... remove the message from its current list and add it to send waiting get list */
                bmip_gen_mutex_lock(&list_mutex);
                qlist_del(&sq->list);
                bmip_gen_mutex_unlock(&list_mutex);
                qlist_add_tail(&sq->list, &q_send_waiting_get);
            }
            break;
        }

        /* get operation event types */
        case PTL_EVENT_GET_START:
        {
	        break;
        }
        case PTL_EVENT_GET_END:
        {
            /* check if this message was a long, nonprepost */
            int found = 0;
            //bmip_gen_mutex_lock(&list_mutex);
            qlist_for_each_entry_safe(sq, sqn, &q_send_waiting_get, list)
            {
                /* if the buffer request matches, mark as found and remove it from the list */
                if(addr_from_nidpid(ev->initiator) == sq->mop.addr && sq->bmi_tag == (ev->match_bits & 0xffffffffULL) && sq->match_bits == ev->match_bits)
                {
                    found = 1;
                    break;
                }
            }

            /* if the event was on the waiting_get list */
            if(found)
            {
                /* remove and add it to the done list */
                bmip_gen_mutex_lock(&list_mutex);
                qlist_del(&sq->list);
                qlist_add_tail(&sq->list, &q_done);
                bmip_gen_mutex_unlock(&list_mutex);
            }
            /* it was not on th waiting_get list */
            else
            {
                sq = ev->md.user_ptr;
                sq->state = SQ_WAITING_USER_TEST;
                sq->actual_len = ev->mlength;

                /* delete request from the current list it is on and add it to the done list */
                bmip_gen_mutex_lock(&list_mutex);
                qlist_del(&sq->list);
                qlist_add_tail(&sq->list, &q_done);
                bmip_gen_mutex_unlock(&list_mutex);
            }
            //bmip_gen_mutex_unlock(&list_mutex);
            break;
        }
        case PTL_EVENT_REPLY_START:
	        break;
        case PTL_EVENT_REPLY_END:
        {
            rq = ev->md.user_ptr;
            rq->state = RQ_WAITING_USER_TEST;
            rq->actual_len = ev->mlength;
 
            /* delete request from the current list it is on and add it to the done list */
            bmip_gen_mutex_lock(&list_mutex);
            qlist_del(&rq->list);
            qlist_add_tail(&rq->list, &q_done);
            bmip_gen_mutex_unlock(&list_mutex);
	        break;
        }

        /* unlink event */
        case PTL_EVENT_UNLINK:
        {
	        break;
        }

        default:
	        bmip_fprintf(stderr, "%s: error, can not handle message\n", __func__);
	        return -EIO;
    };
    return 0;
}

/*
 * Try to drain everything off the EQ.  No idling can be done in here
 * because we have to hold the eq lock for the duration, but if a recv
 * post comes in, it wants to do the post now, not wait another 10 ms.
 *
 * If idle_ms == PTL_TIME_FOREVER, block until an event happens.  This is only
 * used for PtlMDUpdate where we cannot progress until the event is delivered.
 * Portals will increase the eq pending count when the first part of a message
 * comes in, and not generate an event until the end.  This could be lots of
 * packets.  Sorry this could introduce very long delays.
 *
 * Under the hood, utcp cannot implement the timeout anyway.
 */
static int __check_eq(int idle_ms)
{
    ptl_event_t ev;
    int ret = 0;
    int i = 0;
    int ms = idle_ms;

    /* while there are events on the event queue and no errors are encountered */
    for (;;) 
    {
        /* check the portals event queue */
	    ret = PtlEQPoll(&eq, 1, ms, &ev, &i);

        /* if the poll function returned success */
	    if (ret == PTL_OK || ret == PTL_EQ_DROPPED) 
        {
            /* protect the event handler and handle the event */
	        handle_event(&ev);

	        ms = 0;  /* just quickly pull events off */

            /* an event was dropped because of space limits on the event queue */
	        if (ret == PTL_EQ_DROPPED) 
            {
		        /* oh well, hope things retry, just point this out */
		        gossip_err("%s: PtlEQPoll: dropped some completions\n",
			        __func__);
	        }
	    } 
        /* timeout was reached and the event queue is empty */
        else if (ret == PTL_EQ_EMPTY) 
        {
	        ret = 0;
	        break;
	    }
        /* all other errors */ 
        else 
        {
	        ret = -EIO;
	        break;
	    }
    }

    return ret;
}

/*
 * While doing a post_recv(), we have to make sure no events get processed
 * on the EQ.  But other pvfs threads might call testunexpected() etc.  There
 * is a mutex for the eq that wraps check_eq().  A separate lock/release eq
 * is used to block out other senders or eq checkers during a send.
 */
static int check_eq(int idle_ms)
{
    int ret = 0;

    /* protect the eq and check it */
    //bmip_gen_mutex_lock(&list_mutex);
    bmip_gen_mutex_lock(&eq_mutex);
    ret = __check_eq(idle_ms);
    bmip_gen_mutex_unlock(&eq_mutex);
    //bmip_gen_mutex_unlock(&list_mutex);

    return ret;
}

/*
 * Used by testcontext and test.  Called with the list lock held.
 */

/* assumes caller has the list_mutex lock */
static void fill_done(struct bmip_work *w, bmi_op_id_t *id, bmi_size_t *size,
		      void **user_ptr, bmi_error_code_t *err)
{
    *id = w->mop.op_id;
    *size = (w->type == BMI_SEND) ? w->tot_len : w->actual_len;
    if (user_ptr)
	*user_ptr = w->mop.user_ptr;
    *err = 0;
    if (w->state == SQ_CANCELLED || w->state == RQ_CANCELLED)
	*err = -PVFS_ETIMEDOUT;
    if (w->state == RQ_LEN_ERROR)
	*err = -PVFS_EOVERFLOW;

    /* free resources too */
    id_gen_fast_unregister(w->mop.op_id);
}

static int bmip_testcontext(int incount, bmi_op_id_t *outids, int *outcount,
			    bmi_error_code_t *errs, bmi_size_t *sizes,
			    void **user_ptrs, int max_idle_time,
			    bmi_context_id context_id __unused)
{
    struct bmip_work *w; 
    struct bmip_work *wn;
    int ret = 0; 
    int n = 0;
    int timeout = 0;
    int etime = 0;

    if(max_idle_time > 100)
        max_idle_time = 100;

    pthread_yield();
    /* while the timeout has not expired and we are still waiting for operations to complete */
    for (;;) 
    {
	    /*
	     * Poll once quickly to grab some completions.  Then if nothing
	     * has finished, come back and wait for the timeout.
	     */
        //fprintf(stderr, "%s:%i timeout = %i, etime = %i\n", __func__, __LINE__, timeout, etime);
	    bmip_gen_mutex_lock(&eq_mutex);
	    ret = __check_eq(timeout);
	    bmip_gen_mutex_unlock(&eq_mutex);
        //fprintf(stderr, "%s:%i after poll\n", __func__, __LINE__);

        /* update the elapsed time */
        etime += timeout;

        /* exit */
	    if (ret)
	        goto out;

        /* if this was not the quick pass, double the last timeout */
        if(timeout != 0)
        {
            timeout *= 2;
        }
        /* else, set the timeout to 1ms */
        else
        {
            timeout = 20;
        }

        /* if the next timeout will exceed the max idle time */
        if(etime + timeout > max_idle_time)
        {
            timeout = max_idle_time - etime;
        }

        //fprintf(stderr, "%s:%i before done list scan\n", __func__, __LINE__);
        /* protect the done event list */
	    bmip_gen_mutex_lock(&list_mutex);
        int cur_count = 0;
        int counter = qlist_count(&q_done);
        while(!qlist_empty(&q_done) && cur_count < counter)
        {
            struct bmip_multi_work * wmp = NULL, * wmpn = NULL;
            int found = 0;
            bmi_size_t accum_size = 0;
            void * w_user_ptr = NULL;
            int islast = 0;

            /* inc the counter */
            cur_count++;

            //fprintf(stderr, "%s:%i cur_count = %i\n", __func__, __LINE__, cur_count);

            /* if we found n events, break out */
	        if (n == incount)
		        break;

            /* get an item from the done list */
            w = qlist_pop(&q_done);

            //fprintf(stderr, "%s:%i item in list = %p\n", __func__, __LINE__, w);

            /* check the multi part queue */    
            qlist_for_each_entry_safe(wmp, wmpn, &q_mp_done, list)
            {
                //fprintf(stderr, "%s:%i item in list\n", __func__, __LINE__);
                if(wmp->id == w->mop.op_id)
                {
                    found = 1;
                    w_user_ptr = wmp->user_ptr;
                    break;
                }
            }

            if(found)
            {
                qlist_del(&wmp->list);

                struct bmip_master_mp * mp = NULL, * mpn = NULL;
                found = 0;
                //fprintf(stderr, "%s:%i before search mp\n", __func__, __LINE__);
                qlist_for_each_entry_safe(mp, mpn, &q_master_mp, list)
                {
                    if(mp->master_id == wmp->master_id)
                    {
                        mp->count--;
                        if(mp->accum_size < wmp->accum_bytes)
                            mp->accum_size = wmp->accum_bytes;
                        found = 1;
                        break;
                    }
                }
                //fprintf(stderr, "%s:%i after search mp\n", __func__, __LINE__);

                if(found)
                {
                    //fprintf(stderr, "%s:%i found mp\n", __func__, __LINE__);
                    if(mp->count == 0)
                    {
                        //fprintf(stderr, "%s:%i last mp\n", __func__, __LINE__);
                        islast = 1;
                        qlist_del(&mp->list);
                        accum_size = mp->accum_size;
                        BMIP_WORK_MULTI_MASTER_FREE(mp);       
                    }
                }

                if(wmp->index + 1 != wmp->total)
                {
                    //fprintf(stderr, "%s:%i unreg work\n", __func__, __LINE__);
                    id_gen_fast_unregister(&wmp->id);
                }
                else if(wmp->index == 0 && wmp->total == 1)
                {
                    //fprintf(stderr, "%s:%i is last 2\n", __func__, __LINE__);
                    islast = 1;
                }

                BMIP_WORK_MULTI_FREE(wmp);
            }
            else
            {
                qlist_add_tail(&w->list, &q_done);
                continue;
#if 0
                if(counter == cur_count)
                    break;
                else
                    continue;
#endif
            }

            if(islast)
            { 
                //fprintf(stderr, "%s:%i fill done values\n", __func__, __LINE__);
                /* fill the completed events */
	            fill_done(w, &outids[n], &sizes[n],
		            user_ptrs ? &user_ptrs[n] : NULL, &errs[n]);
     
                if(accum_size != 0)
                    sizes[n] = accum_size;
                if(user_ptrs)
                {
                    if(w_user_ptr)
                        user_ptrs[n] = w_user_ptr;
                }

                /* inc the counter */
                ++n;
            }

            /* remove item from the list */
            //fprintf(stderr, "%s:%i before free\n", __func__, __LINE__);
            BMIP_WORK_FREE(w);
            w = NULL;
            //fprintf(stderr, "%s:%i after free\n", __func__, __LINE__);
        }
	    bmip_gen_mutex_unlock(&list_mutex);
        //fprintf(stderr, "%s:%i after done list scan\n", __func__, __LINE__);

        /* some entries completed or we hit the idle time... break out */ 
	    if (n > 0 || etime == max_idle_time)
	        break;

        int ucount = 0;
        //fprintf(stderr, "%s:%i before unexp list scan\n", __func__, __LINE__);
	    bmip_gen_mutex_lock(&list_mutex);
        ucount = qlist_count(&q_unexpected_done);
	    bmip_gen_mutex_unlock(&list_mutex);
        //fprintf(stderr, "%s:%i after unexp list scan\n", __func__, __LINE__);

        if(ucount > 0)
            break;
    } 
out:
    //fprintf(stderr, "%s:%i exit\n", __func__, __LINE__);
    *outcount = n;
    return ret;
}

/*
 * Used by lots of BMI test codes, but never in core PVFS.  Easy enough
 * to implement though.
 */
static int bmip_test(bmi_op_id_t id, int *outcount, bmi_error_code_t *err,
		     bmi_size_t *size, void **user_ptr, int max_idle_time,
		     bmi_context_id context_id __unused)
{
    struct bmip_work *w, *wn;
    int ret = 0, n = 0;
    int timeout = 0;

	//bmip_gen_mutex_lock(&list_mutex);
    for (;;)
    {
	    ret = check_eq(timeout);
	    if (ret)
	        goto out;

	    bmip_gen_mutex_lock(&list_mutex);
	    qlist_for_each_entry_safe(w, wn, &q_done, list) 
        {
	        if (w->mop.op_id == id) 
            {
		        fill_done(w, &id, size, user_ptr, err);
		        ++n;
		        break;
	        }
	    }
	    bmip_gen_mutex_unlock(&list_mutex);

	    if (n > 0 || timeout == max_idle_time)
	        break;

	    timeout = max_idle_time;
    }

out:
	//bmip_gen_mutex_unlock(&list_mutex);
    *outcount = n;
    return ret;
}

/*
 * Only used by one BMI test program, not worth the code space to implement.
 */
static int bmip_testsome(int num __unused, bmi_op_id_t *ids __unused,
			 int *outcount __unused, int *other_thing __unused,
			 bmi_error_code_t *err __unused,
			 bmi_size_t *size __unused, void **user_ptr __unused,
			 int max_idle_time __unused,
			 bmi_context_id context_id __unused)
{
    gossip_err("%s: unimplemented\n", __func__);
    return bmi_errno_to_pvfs(-ENOSYS);
}

/*
 * Check the EQ, briefly, then return any unexpecteds, then wait for up
 * to the idle time.
 */
static int bmip_testunexpected(int incount, int *outcount,
			       struct bmi_method_unexpected_info *ui,
			       int max_idle_time)
{
    struct bmip_work *w, *wn;
    int ret = 0, n = 0;
    int timeout = 0;
    int etime = 0;

    pthread_yield();
    for (;;) 
    {
        bmip_gen_mutex_lock(&eq_mutex);
	    ret = __check_eq(timeout);
        bmip_gen_mutex_unlock(&eq_mutex);

        etime += timeout;

        if(timeout == 0)
        {
            timeout = 1;
        }	
        else
        {
            timeout *= 2;
        }

        /* if the next timeout will exceed the max idle time */
        if(etime + timeout > max_idle_time)
        {
            timeout = max_idle_time - etime;
        }

        if (ret)
	        goto out;

        bmip_gen_mutex_lock(&list_mutex);
	    qlist_for_each_entry_safe(w, wn, &q_unexpected_done, list)
        {
	        if (n == incount)
		        break;

    	    ui[n].error_code = 0;
	        ui[n].addr = w->mop.addr;
	        ui[n].size = w->actual_len;
    	    ui[n].buffer = w->unex_buf;  /* preallocated above */
	        ui[n].tag = w->bmi_tag;
	        qlist_del(&w->list);
	        BMIP_WORK_FREE(w);
	        ++n;
	    }
        bmip_gen_mutex_unlock(&list_mutex);

	    if(n > 0 || max_idle_time == etime)
        {
	        break;
        }

	    //timeout = max_idle_time;
    }

out:
    *outcount = n;
    return ret;
}


/*----------------------------------------------------------------------------
 * Send
 */

/*
 * Clients do not open the NIC until they go to connect to a server.
 * In theory, which server could dictate which NIC.
 *
 * Server also calls this to initialize, but with a non-ANY pid.
 */
static int ensure_ni_initialized(struct bmip_method_addr *peer __unused,
				 ptl_process_id_t my_pid)
{
    int ret = 0;
    ptl_process_id_t no_pid;
    int nic_type;
    ptl_md_t zero_mdesc = {
	.threshold = PTL_MD_THRESH_INF,
	.max_size = 0,
	.options = PTL_MD_OP_PUT | PTL_MD_TRUNCATE | PTL_MD_MAX_SIZE,
		   // | PTL_MD_EVENT_START_DISABLE,
	.user_ptr = 0,
    };

    /* already initialized */
    if (ni != PTL_INVALID_HANDLE)
	return ret;

    bmip_gen_mutex_lock(&ni_mutex);

    /* check again now that we have the mutex */
    if (ni != PTL_INVALID_HANDLE)
	goto out;

    /*
     * XXX: should do this properly.  If server, we could look
     * up our listen address and get an interface from that.  If client,
     * lookup server, figure out how route would go to it, choose
     * that interface.  Yeah.
     */

#if defined(__CRAYXT_SERVICE) || defined(__CRAYXT_COMPUTE_LINUX_TARGET)
#warning "Build with nic_type = CRAY_USER_NAL"
    /*
     * Magic for Cray XT service nodes and compute node linux.
     * Catamount uses default, TCP uses default.
     */
    nic_type = CRAY_USER_NAL;
    //nic_type = CRAY_ACCEL;
#else
#warning "Build with nic_type = PTL_IFACE_DEFAULT"
    nic_type = PTL_IFACE_DEFAULT;
#endif

    /* needed for TCP */
    /* setenv("PTL_IFACE", "eth0", 0); */

    //ret = PtlNIInit(nic_type, my_pid.pid, NULL, NULL, &ni);
    ret = PtlNIInit(IFACE_FROM_BRIDGE_AND_NALID(PTL_BRIDGE_UK,PTL_IFACE_SS), my_pid.pid, NULL, NULL, &ni);
#if defined(__LIBCATAMOUNT__) || defined(__CRAYXT_COMPUTE_LINUX_TARGET)
    if (ret == PTL_IFACE_DUP && ni != PTL_INVALID_HANDLE) {
	ret = 0;  /* already set up by pre-main on catamount nodes */
	ni_init_dup = 1;
    }
#endif
    if (ret) {
	/* error number is bogus here, do not try to decode it */
	gossip_err("%s: PtlNIInit failed: %s\n", __func__, PtlErrorStr(ret));
	ni = PTL_INVALID_HANDLE;  /* init call nulls it out */
	ret = -EIO;
	goto out;
    }

    /*
     * May not be able to assign PID to whatever we want.  Let's see
     * what runtime has assigned.
     */
    {
    ptl_process_id_t id;
    ret = PtlGetId(ni, &id);

    if (ret != 0) {
	gossip_err("%s:%i PtlGetId failed: ret = %d , str = %s, id = %p\n", __func__, ret, __LINE__, PtlErrorStr(ret), &id);
	ni = PTL_INVALID_HANDLE;
	ret = -EIO;
	goto out;
    }
    //debug(0, "%s: runtime thinks my id is %d.%d\n", __func__, id.nid, id.pid);
    }

#if !(defined(__LIBCATAMOUNT__) || defined(__CRAYXT_SERVICE) || defined(__CRAYXT_COMPUTE_LINUX_TARGET))
    /*
     * Need an access control entry to allow everybody to talk, else root
     * cannot talk to random user, e.g.  Not implemented on Cray.
     */
#ifdef HAVE_PTLACENTRY_JID
    ret = PtlACEntry(ni, 0, any_pid, (ptl_uid_t) -1, (ptl_jid_t) -1, ptl_index);
#else
    ret = PtlACEntry(ni, 0, any_pid, (ptl_uid_t) -1, ptl_index);
#endif
    if (ret) {
	gossip_err("%s: PtlACEntry: %s\n", __func__, PtlErrorStr(ret));
	ret = -EIO;
	goto out;
    }
#endif

    /* a single EQ for all messages, with some arbitrary depth */
    ret = PtlEQAlloc(ni, 100000, PTL_EQ_HANDLER_NONE, &eq);
    if (ret) {
	gossip_err("%s: PtlEQAlloc: %s\n", __func__, PtlErrorStr(ret));
	ret = -EIO;
	goto out;
    }

    /* "mark" match entry that denotes the bottom of the prepost entries */
    no_pid.nid = 0;
    no_pid.pid = 0;
    ret = PtlMEAttach(ni, ptl_index, no_pid, 0, 0, PTL_RETAIN, PTL_INS_BEFORE,
		      &mark_me);
    if (ret) {
	gossip_err("%s: PtlMEAttach mark: %s\n", __func__, PtlErrorStr(ret));
	ret = -EIO;
	goto out;
    }

    /* "zero" grabs just the header (of nonprepost, not unexpected), drops the
     * contents */
    ret = PtlMEAttach(ni, ptl_index, any_pid, 0,
		      (0x3fffffffULL << 32) | 0xffffffffULL, PTL_RETAIN,
		      PTL_INS_AFTER, &zero_me);
    if (ret) {
	gossip_err("%s: PtlMEAttach zero: %s\n", __func__, PtlErrorStr(ret));
	ret = -EIO;
	goto out;
    }

    zero_mdesc.eq_handle = eq;
    ret = PtlMDAttach(zero_me, zero_mdesc, PTL_RETAIN, &zero_md);
    if (ret) {
	gossip_err("%s: PtlMDAttach zero: %s\n", __func__, PtlErrorStr(ret));
	ret = -EIO;
	goto out;
    }

    /* now it is time to build this queue, once per NI */
    nonprepost_init();

out:
    if (ret) {
	if (mark_me != PTL_INVALID_HANDLE)
	    PtlMEUnlink(zero_me);
	if (mark_me != PTL_INVALID_HANDLE)
	    PtlMEUnlink(mark_me);
	if (eq != PTL_INVALID_HANDLE)
	    PtlEQFree(eq);
	if (ni != PTL_INVALID_HANDLE)
	    PtlNIFini(ni);
    }
    bmip_gen_mutex_unlock(&ni_mutex);
    return ret;
}

/*
 * Fill in bits for BMI, used by caller for later test or cancel.
 */
static void fill_mop(struct bmip_work *w, bmi_op_id_t *id,
		     struct bmi_method_addr *addr, void *user_ptr,
		     bmi_context_id context_id)
{
    id_gen_fast_register(&w->mop.op_id, &w->mop);
    w->mop.addr = addr;
    w->mop.method_data = w;
    w->mop.user_ptr = user_ptr;
    w->mop.context_id = context_id;
    *id = w->mop.op_id;
}

static void cleanup_mdesc(struct bmip_work *w)
{
    if((w->mdesc_options & PTL_MD_IOVEC) != 0 && w->mdesc_length > 1)
    {
        unsigned int i = 0;
        for(i = 0 ; i < w->mdesc_length ; i++)
        {
            if(w->alignbm[i])
            {
                bmip_free(((ptl_md_iovec_t *)w->mdesc_start)[i].iov_base);
            }
        }
        bmip_free(w->alignbm);
        bmip_free(w->mdesc_start);
    }
}

/*
 * Initialize an mdesc for a get or put, either sq or rq side.
 */
static void build_mdesc(struct bmip_work *w, ptl_md_t *mdesc, int numbufs,
			void *const *buffers, const bmi_size_t *sizes)
{
    //mdesc->threshold = 1;
    mdesc->threshold = PTL_MD_THRESH_INF;
    mdesc->options = 0;  /* PTL_MD_EVENT_START_DISABLE; */
    mdesc->eq_handle = eq;
    mdesc->user_ptr = w;

    /* if there are multiple buffers */
    if (numbufs > 1)
    {
	    int i;
    	ptl_md_iovec_t * iov = NULL;

        iov = (ptl_md_iovec_t *)bmip_malloc(sizeof(ptl_md_iovec_t) * numbufs);
        w->alignbm = (char *)bmip_malloc(sizeof(char) * numbufs);

        /* for each buffer */
	    for (i = 0 ; i < numbufs ; i++) 
        {
            /* check for alignment... if unaligned, align it */
            if(((uintptr_t)buffers[i] & 7) != 0)
            {
                void * abuffer = NULL;

                w->alignbm[i] = 1;

                /* allocate aligned mem */
                posix_memalign(&abuffer, BMIP_ALIGN, sizes[i]);

                /* copy the data from the unaligned buffer into the aligned buffer */
                memcpy(abuffer, buffers[i], sizes[i]);

                /* init the iovec */
	            iov[i].iov_base = abuffer;
	            iov[i].iov_len = sizes[i];
            }
            /* else, just copy the buffer pointer */
            else
            {
                w->alignbm[i] = 0;
	            iov[i].iov_base = buffers[i];
	            iov[i].iov_len = sizes[i];
            }
	    }
	    mdesc->options |= PTL_MD_IOVEC;
	    mdesc->start = (void *) iov;
	    mdesc->length = numbufs;
    }
    /* else, this was a single buffer */ 
    else 
    {
	    mdesc->start = *buffers;
	    mdesc->length = *sizes;
    }

    /* set the data into the w in case we need to rebuild later */
    w->mdesc_length = mdesc->length;
    w->mdesc_start = mdesc->start;
    w->mdesc_options = mdesc->options;
}

static void build_mdesc_work(struct bmip_work *w, ptl_md_t *mdesc)
{
    //mdesc->threshold = 1;
    mdesc->threshold = PTL_MD_THRESH_INF;
    mdesc->options = 0;  /* PTL_MD_EVENT_START_DISABLE; */
    mdesc->eq_handle = eq;
    mdesc->user_ptr = w;

    /* if there are multiple buffers */
    if(w->user_numbufs > 1)
    {
	    int i;
        /* iovec list starts after the bmip_work struct */
    	//ptl_md_iovec_t *iov = (void *) &w[1];
    	ptl_md_iovec_t * iov = NULL;

        iov = (ptl_md_iovec_t *)bmip_malloc(sizeof(ptl_md_iovec_t) * w->user_numbufs);
        w->alignbm = (char *)bmip_malloc(sizeof(char) * w->user_numbufs);

        /* for each buffer */
	    for (i = 0 ; i < w->user_numbufs ; i++) 
        {
            /* check for alignment... if unaligned, align it */
            if(((uintptr_t)((char **)w->user_buffers)[i] & 7) != 0)
            {
                void * abuffer = NULL;

                w->alignbm[i] = 1;

                /* allocate aligned mem */
                posix_memalign(&abuffer, BMIP_ALIGN, w->user_sizes[i]);

                /* copy the data from the unaligned buffer into the aligned buffer */
                memcpy(abuffer, ((char **)w->user_buffers)[i], w->user_sizes[i]);

                /* init the iovec */
	            iov[i].iov_base = abuffer;
	            iov[i].iov_len = w->user_sizes[i];
            }
            /* else, just copy the buffer pointer */
            else
            {
                w->alignbm[i] = 0;

	            iov[i].iov_base = ((char **)w->user_buffers)[i];
	            iov[i].iov_len = w->user_sizes[i];
            }
	    }
	    mdesc->options |= PTL_MD_IOVEC;
	    mdesc->start = (void *) iov;
	    mdesc->length = w->user_numbufs;
    }
    /* else, this was a single buffer */ 
    else 
    {
	    mdesc->start = w->user_buffers;
	    mdesc->length = *(w->user_sizes);
    }

    /* set the data into the w in case we need to rebuild later */
    w->mdesc_length = mdesc->length;
    w->mdesc_start = mdesc->start;
    w->mdesc_options = mdesc->options;
}

/*
 * Generic interface for both send and sendunexpected, list and non-list send.
 */
static int
post_send(bmi_op_id_t *id, struct bmi_method_addr *addr,
	  int numbufs, const void *const *buffers, const bmi_size_t *sizes,
	  bmi_size_t total_size, bmi_msg_tag_t bmi_tag, void *user_ptr,
	  bmi_context_id context_id, int is_unexpected, uint32_t subseqno)
{
    struct bmip_method_addr *pma = addr->method_data;
    struct bmip_work *sq;
    uint64_t mb;
    int ret = 0;
    int i = 0;
    ptl_md_t mdesc;

    /* unexpected messages must fit inside the agreed limit */
    if (is_unexpected && total_size > UNEXPECTED_MESSAGE_SIZE)
    {
	    ret = -EINVAL;
        goto out;
    }

    bmip_gen_mutex_lock(&eq_mutex);  /* do not let test threads manipulate eq */
    ret = ensure_ni_initialized(pma, any_pid);
    bmip_gen_mutex_unlock(&eq_mutex);
    if (ret)
	goto out;

    BMIP_WORK_ALLOC(sq);
    if (!sq) {
	ret = -ENOMEM;
	goto out;
    }
    sq->type = BMI_SEND;
    sq->saw_send_end_and_ack = 0;
    sq->tot_len = total_size;
    sq->is_unexpected = is_unexpected;
    fill_mop(sq, id, addr, user_ptr, context_id);
    /* lose cast to fit in non-const portals iovec */
    build_mdesc(sq, &mdesc, numbufs, (void *const *)(uintptr_t) buffers, sizes);
    //mdesc.threshold = 3;  /* put, ack */
    mdesc.threshold = PTL_MD_THRESH_INF;  /* put, ack */

    /* setup the user buffer storage */
    if(sq->user_buffers == NULL)
    {
        if(numbufs < 2)
        {
            sq->user_buffers = *(char * const*)buffers;;
        }
        else
        {
            sq->user_buffers = bmip_malloc(sizeof(void *) * numbufs);
            int n = 0;
            for(n = 0 ; n < numbufs ; n++)
            {
                ((char**)sq->user_buffers)[n] = ((char * const*)buffers)[n];
            }
        }
    }
    if(sq->user_sizes == NULL)
    {
        /* copy a single value */
        if(numbufs < 2)
        {
            sq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t));
            *(sq->user_sizes) = *sizes;
        }
        /* copy all of the elements in the user size list to the request */
        else
        {
            sq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t) * numbufs);
            int n = 0;
            for(n = 0 ; n < numbufs ; n++)
            {
                sq->user_sizes[n] = sizes[n];
            }
        }
    }
    if(sq->user_total_size == 0)
    {
        sq->user_total_size = total_size;
    }
    if(sq->user_numbufs == 0)
    {
        sq->user_numbufs = numbufs;
    }

    sq->state = SQ_WAITING_ACK;
    bmip_gen_mutex_lock(&list_mutex);
    qlist_add_tail(&sq->list, &q_send_waiting_ack);
    bmip_gen_mutex_unlock(&list_mutex);

    /* if not unexpected, use an ME in case he has to come get it */
    if (sq->is_unexpected) 
    {
	    /* md without any match entry, for sending */
	    mb = match_bits_unexpected | bmi_tag;

        /* if the seqno reset flag was set... */
        if(reset_seqno_flag)
        {
            /* set the reset seqno match bit */
            mb = mb | match_bits_reset_seq;
            reset_seqno_flag = 0;
        }
	    mdesc.options |= PTL_MD_OP_GET | PTL_MD_OP_PUT;
	    //mdesc.threshold = 3;  /* put, ack, maybe get */
	    mdesc.threshold = PTL_MD_THRESH_INF;  /* put, ack, maybe get */
	    ret = PtlMDBind(ni, mdesc, PTL_UNLINK, &sq->md);
	    if (ret) 
        {
	        gossip_err("%s: PtlMDBind: %s\n", __func__, PtlErrorStr(ret));
            bmip_fprintf(stderr, "%s : exit, -EIO, PtlMDBind failed: %s\n", __func__, PtlErrorStr(ret));
	        ret = -EIO;
            goto out;
	    }
        sq->manual_md_unlink = 1;
    } 
    else 
    {
	    /* seqno increments on every expected send (only) */
	    if (++pma->seqno_out >= match_bits_seqno_max)
	        pma->seqno_out = 0;
	    mb = mb_from_tag_and_seqno_and_subseqno(bmi_tag, pma->seqno_out, subseqno);
	    /* long-send bit only on the ME, not as the outgoing mb in PtlPut */
        //fprintf(stderr, "%s:%i insert ME\n", __func__, __LINE__);
	    ret = PtlMEInsert(mark_me, pma->pid, match_bits_long_send | mb,
			  0, PTL_UNLINK, PTL_INS_BEFORE, &sq->me);
	    if (ret) 
        {
	        gossip_err("%s: PtlMEInsert: %s\n", __func__, PtlErrorStr(ret));
            fprintf(stderr, "%s:%i : exit, -EIO, PtlMEInsert failed: %s\n", __func__, __LINE__, PtlErrorStr(ret));
	        ret = -EIO;
            goto out;
	    }
        sq->manual_me_unlink = 1;

	    /*
	     * Truncate is used to send just what we have, which may be less
	     * than he wants to receive.  Otherwise would have to chop down the
	     * iovec list to fit on the Get-ter, or use GetRegion.
	     */
	    //mdesc.options |= PTL_MD_OP_GET | PTL_MD_TRUNCATE | PTL_MD_EVENT_START_DISABLE;
	    mdesc.options |= PTL_MD_OP_GET | PTL_MD_OP_PUT | PTL_MD_TRUNCATE;
	    //mdesc.threshold = 3;  /* put, ack, maybe get */
	    mdesc.threshold = PTL_MD_THRESH_INF;  /* put, ack, maybe get */
	    ret = PtlMDAttach(sq->me, mdesc, PTL_UNLINK, &sq->md);
	    if (ret) 
        {
	        gossip_err("%s: PtlMDAttach: %s\n", __func__, PtlErrorStr(ret));
            bmip_fprintf(stderr, "%s : exit, -EIO, PtlMDAttach failed: %s\n", __func__, PtlErrorStr(ret));
	        ret = -EIO;
            goto out;
	    }
        sq->manual_md_unlink = 1;
    }

    sq->bmi_tag = bmi_tag;  /* both for debugging dumps */
    sq->match_bits = mb;

    ret = PtlPut(sq->md, PTL_ACK_REQ, pma->pid, ptl_index, 0, mb, 0, 0);
    if (ret) 
    {
	    gossip_err("%s: PtlPut: %s\n", __func__, PtlErrorStr(ret));
        bmip_fprintf(stderr, "%s : exit, -EIO, PtlPut failed: %s\n", __func__, PtlErrorStr(ret));
	    ret = -EIO;
        goto out;
    }

out:
    return ret;
}

static int bmip_post_send(bmi_op_id_t *id, struct bmi_method_addr *remote_map,
			  const void *buffer, bmi_size_t total_size,
			  enum bmi_buffer_type buffer_flag __unused,
			  bmi_msg_tag_t tag, void *user_ptr,
			  bmi_context_id context_id)
{
    int ret = 0;
    struct bmip_multi_work * mpw = NULL, * mpwn = NULL;
    QLIST_HEAD(q_send_list);

#ifndef BMIPTLSEG
        BMIP_WORK_MULTI_ALLOC(mpw);

        ret = post_send(id, remote_map, 1, &buffer, &total_size,
            total_size, tag, user_ptr, context_id, 0, 0);

        /* setup multi part work unit */
        mpw->id = *id;
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->user_ptr = user_ptr;
        mpw->accum_bytes = total_size;
        bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
        bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);
        return ret;
#else
    bmip_gen_mutex_lock(&op_mutex);
    if(total_size <= BMIP_MAX_SEGMENT_SIZE)
    {
        BMIP_WORK_MULTI_ALLOC(mpw);

        ret = post_send(id, remote_map, 1, &buffer, &total_size,
	        total_size, tag, user_ptr, context_id, 0, 0);

        /* setup multi part work unit */
        mpw->id = *id;
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->user_ptr = user_ptr;
        mpw->accum_bytes = total_size;
	    bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
	    bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);
        return ret;
    }
    else
    {
        bmi_size_t tbytes = 0;
        void * cur_buffer = NULL;
        bmi_size_t cur_off = 0;
        bmi_size_t cur_size = 0;
        int is_last = 0;
        bmi_size_t accum_bytes = 0;
        struct bmip_multi_work * mpw = NULL;
        uint32_t subindex = 0;

        while(tbytes < total_size)
        {
            if(total_size - tbytes >= BMIP_MAX_SEGMENT_SIZE)
            {
                cur_size = BMIP_MAX_SEGMENT_SIZE;
                accum_bytes += cur_size;
            }
            else
            {
                cur_size = total_size - tbytes;
            }

            /* update the transfer bytes count */
            tbytes += cur_size;

            /* check if this is the last transfer */
            if(tbytes == total_size)
                is_last = 1;

            /* update the buffer */
            cur_buffer = buffer + cur_off; 
            cur_off += BMIP_MAX_SEGMENT_SIZE;

            /* send the buffer */
            if(!is_last)
            {
                /* set the user_ptr to NULL and do not use the master ID */
                BMIP_WORK_MULTI_ALLOC(mpw);
                ret = post_send(&mpw->id, remote_map, 1, &cur_buffer, &cur_size,
	                cur_size, tag, NULL, context_id, 0, subindex);

                /* setup multi part work unit */
                mpw->tag = tag;
                mpw->index = subindex;
                mpw->user_ptr = user_ptr;
                mpw->accum_bytes = accum_bytes;
	            bmip_gen_mutex_lock(&list_mutex);
                qlist_add_tail(&mpw->list, &q_send_list);
	            bmip_gen_mutex_unlock(&list_mutex);
                mpw = NULL;
            }
            else
            {
                ret = post_send(id, remote_map, 1, &cur_buffer, &cur_size,
	                cur_size, tag, user_ptr, context_id, 0, subindex);

                /* setup multi part work unit */
                BMIP_WORK_MULTI_ALLOC(mpw);
                mpw->id = *id;
                mpw->index = subindex;
                mpw->user_ptr = user_ptr;
                mpw->accum_bytes = accum_bytes;
	            bmip_gen_mutex_lock(&list_mutex);
                qlist_add_tail(&mpw->list, &q_send_list);
	            bmip_gen_mutex_unlock(&list_mutex);
            }
            subindex++;
        }

        /* transfer the local list to the master list */
        bmip_gen_mutex_lock(&list_mutex);
        qlist_for_each_entry_safe(mpw, mpwn, &q_send_list, list)
        {
            /* set the segment total */
            mpw->total = subindex;

            /* set the master op id */
            mpw->master_id = *id;
            
            mpw->user_ptr = user_ptr;

            /* remove from the local list */
            qlist_del(&(mpw->list));

            /* add it to the master list */
            qlist_add_tail(&mpw->list, &q_mp_done);
        }

        struct bmip_master_mp * m = NULL;
        BMIP_WORK_MULTI_MASTER_ALLOC(m);        
        m->master_id = *id;
        m->count = subindex;
        qlist_add_tail(&m->list, &q_master_mp);

        bmip_gen_mutex_unlock(&list_mutex);
    }
    bmip_gen_mutex_unlock(&op_mutex);
    return ret;
#endif
}

static int bmip_post_send_list(bmi_op_id_t *id, struct bmi_method_addr *remote_map,
			       const void *const *buffers,
			       const bmi_size_t *sizes, int list_count,
			       bmi_size_t total_size,
			       enum bmi_buffer_type buffer_flag __unused,
			       bmi_msg_tag_t tag, void *user_ptr,
			       bmi_context_id context_id)
{
    int ret = 0;
    struct bmip_multi_work * mpw = NULL, * mpwn = NULL;
    QLIST_HEAD(q_send_list);

#ifndef BMIPTLSEG
        BMIP_WORK_MULTI_ALLOC(mpw);

        ret = post_send(id, remote_map, list_count, buffers, sizes,
             total_size, tag, user_ptr, context_id, 0, 0);

        /* setup multi part work unit */
        mpw->id = *id;
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->user_ptr = user_ptr;
        mpw->accum_bytes = total_size;
        bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
        bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);
        return ret;
#else
    bmip_gen_mutex_lock(&op_mutex);
    if(total_size <= BMIP_MAX_SEGMENT_SIZE)
    {
        BMIP_WORK_MULTI_ALLOC(mpw);

        ret = post_send(id, remote_map, list_count, buffers, sizes,
		     total_size, tag, user_ptr, context_id, 0, 0);

        /* setup multi part work unit */
        mpw->id = *id;
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->user_ptr = user_ptr;
        mpw->accum_bytes = total_size;
        bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
        bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);
        return ret;
    }
    else
    {
        int lcount = 0;
        uint32_t subindex = 0;
        bmi_size_t accum_bytes = 0;
        for(lcount = 0 ; lcount < list_count ; lcount++)
        {
            bmi_op_id_t subid;
            bmi_size_t tbytes = 0;
            void * cur_buffer = NULL;
            bmi_size_t cur_off = 0;
            bmi_size_t cur_size = 0;
            int is_last = 0;
            struct bmip_multi_work * mpw = NULL; 
        
            while(tbytes < sizes[lcount])
            {
                if(sizes[lcount] - tbytes >= BMIP_MAX_SEGMENT_SIZE)
                {
                    cur_size = BMIP_MAX_SEGMENT_SIZE;
                    accum_bytes += cur_size;
                }
                else
                {
                    cur_size = sizes[lcount] - tbytes;
                    if(tbytes == 0)
                        accum_bytes += cur_size;
                }

                /* update the transfer bytes count */
                tbytes += cur_size;

                /* check if this is the last transfer */
                if(tbytes == sizes[lcount] && lcount + 1 == list_count)
                    is_last = 1;

                /* update the buffer */
                cur_buffer = buffers[lcount] + cur_off; 
                cur_off += BMIP_MAX_SEGMENT_SIZE;

                /* send the buffer */
                if(!is_last)
                {
                    /* set the user_ptr to NULL and do not use the master ID */
                    BMIP_WORK_MULTI_ALLOC(mpw);
                    ret = post_send(&mpw->id, remote_map, 1, &cur_buffer, &cur_size,
	                    cur_size, tag, NULL, context_id, 0, subindex);

                    /* setup multi part work unit */
                    mpw->tag = tag;
                    mpw->index = subindex;
                    mpw->user_ptr = user_ptr;
                    mpw->accum_bytes = accum_bytes;
                    bmip_gen_mutex_lock(&list_mutex);
                    qlist_add_tail(&mpw->list, &q_send_list);
                    bmip_gen_mutex_unlock(&list_mutex);
                    mpw = NULL;
                }
                else
                {
                    /* use the master ID and user_ptr */
                    BMIP_WORK_MULTI_ALLOC(mpw);
                    ret = post_send(id, remote_map, 1, &cur_buffer, &cur_size,
	                    cur_size, tag, user_ptr, context_id, 0, subindex);

                    /* setup multi part work unit */
                    mpw->id = *id;
                    mpw->tag = tag;
                    mpw->user_ptr = user_ptr;
                    mpw->index = subindex;
                    mpw->accum_bytes = accum_bytes;
                    bmip_gen_mutex_lock(&list_mutex);
                    qlist_add_tail(&mpw->list, &q_send_list);
                    bmip_gen_mutex_unlock(&list_mutex);
                }

                /* inc the subindex */
                subindex++;

            } /* while */
        } /* for */

        /* transfer the local list to the master list */
        bmip_gen_mutex_lock(&list_mutex);
        qlist_for_each_entry_safe(mpw, mpwn, &q_send_list, list)
        {
            /* set the segment total */
            mpw->total = subindex;

            /* set the master op id */
            mpw->master_id = *id;

            mpw->user_ptr = user_ptr;

            /* remove from the local list */
            qlist_del(&(mpw->list));

            /* add it to the master list */
            qlist_add_tail(&mpw->list, &q_mp_done);
        }

        struct bmip_master_mp * m = NULL;
        BMIP_WORK_MULTI_MASTER_ALLOC(m);
        m->master_id = *id;
        m->count = subindex;
        qlist_add_tail(&m->list, &q_master_mp);

        bmip_gen_mutex_unlock(&list_mutex);
    } /* if / else */
    bmip_gen_mutex_unlock(&op_mutex);
    return ret;
#endif
}

static int bmip_post_sendunexpected(bmi_op_id_t *id,
				    struct bmi_method_addr *remote_map,
				    const void *buffer, bmi_size_t total_size,
				    enum bmi_buffer_type bflag __unused,
				    bmi_msg_tag_t tag, void *user_ptr,
				    bmi_context_id context_id)
{
    int ret = 0;
    struct bmip_multi_work * mpw = NULL;

    bmip_gen_mutex_lock(&op_mutex);
    BMIP_WORK_MULTI_ALLOC(mpw);

    /* setup multi part work unit */
    mpw->tag = tag;
    mpw->index = 0;
    mpw->total = 1;
    mpw->user_ptr = user_ptr;

    ret = post_send(id, remote_map, 0, &buffer, &total_size,
		     total_size, tag, user_ptr, context_id, 1, 0);

    mpw->accum_bytes = total_size;
    mpw->id = *id;

    bmip_gen_mutex_lock(&list_mutex);
    qlist_add_tail(&mpw->list, &q_mp_done);
    bmip_gen_mutex_unlock(&list_mutex);

    bmip_gen_mutex_unlock(&op_mutex);
    return ret;
}

static int bmip_post_sendunexpected_list(bmi_op_id_t *id,
					 struct bmi_method_addr *remote_map,
					 const void *const *buffers,
					 const bmi_size_t *sizes,
					 int list_count, bmi_size_t total_size,
					 enum bmi_buffer_type bflag __unused,
					 bmi_msg_tag_t tag, void *user_ptr,
					 bmi_context_id context_id)
{
    int ret = 0;

    bmip_gen_mutex_lock(&op_mutex);
    ret = post_send(id, remote_map, list_count, buffers, sizes,
		     total_size, tag, user_ptr, context_id, 1, 0);
    bmip_gen_mutex_unlock(&op_mutex);
    return ret;
}


/*----------------------------------------------------------------------------
 * Receive
 */

/*
 * Assumes that buf/len will fit in numbufs/buffers/sizes.  For use
 * in copying out of non-preposted buffer area to user-supplied iovec.
 */
static void memcpy_buflist(int numbufs __unused, void *const *buffers,
			   const bmi_size_t *sizes, const void *vbuf,
			   bmi_size_t len)
{
    bmi_size_t thislen;
    const char *buf = vbuf;
    int i = 0;

    while (len)
    {
	    thislen = len;
	    if(thislen > sizes[i])
	        thislen = sizes[i];
	    memcpy(buffers[i], buf, thislen);
	    buf += thislen;
	    len -= thislen;
	    ++i;
    }
}

/*
 * Part of post_recv.  Search the queue of non-preposted receives that the
 * other side has sent to us.  If one matches, satisfy this receive now,
 * else return and let the receive be preposted.
 */
static int match_nonprepost_recv(bmi_op_id_t *id, struct bmi_method_addr *addr,
				 int numbufs, void *const *buffers,
				 const bmi_size_t *sizes,
				 bmi_size_t total_size, bmi_msg_tag_t tag,
				 void *user_ptr, bmi_context_id context_id, uint32_t subseqno)
{
    struct bmip_method_addr *pma = addr->method_data;
    int found = 0;
    int ret = 0;
    ptl_md_t mdesc;
    struct bmip_work *rq, *rqn;
    uint64_t mb;

    /* expected match bits */
    mb = mb_from_tag_and_seqno_and_subseqno(tag, pma->seqno_in, subseqno);

    /* if this is a long message, set the correct bits */
    if(total_size > NONPREPOST_MESSAGE_SIZE)
    {
        mb |= match_bits_long_send;
    }

    /* for each request in the nonprepost q, check for a match */
    /* XXX: remove bmi_tag comparison if match_bits works */

    /* look in the completed nonprepost list */
    //bmip_gen_mutex_lock(&list_mutex);
    qlist_for_each_entry_safe(rq, rqn, &q_recv_nonprepost, list) 
    {
        /* if the buffer request matches, mark as found and remove it from the list */
	    if (rq->mop.addr == addr && rq->bmi_tag == tag && rq->match_bits == mb) 
        {
	        found = 1;
	        break;
	    }
    }
    if(found)
    {
	    qlist_del(&rq->list);
    }
    //bmip_gen_mutex_unlock(&list_mutex);

    /* if the request was not found, exit the nonprepost recv */
    if(!found)
    {
	    goto out;
    }

    /* rq matched, use the addr found at event time */
    fill_mop(rq, id, rq->mop.addr, user_ptr, context_id);

    /* verify length fits */
    if(rq->actual_len > total_size)
    {
	    rq->state = RQ_LEN_ERROR;
	    goto foundout;
    }

    /* short message, copy and release MD buf */
    if(rq->nonpp_buf != NULL) 
    {
        /* copy the message */
	    memcpy_buflist(numbufs, buffers, sizes, rq->nonpp_buf,
		       rq->actual_len);
	    --nonprepost_refcnt[rq->nonpp_md];

        /* unpost some of the slots if they are free */
        if(nonprepost_need_repost_sum)
        {
            /* for each nonprepost message slot */
            int npi = 0;
            for(npi = 0 ; npi < NONPREPOST_NUM_MD ; npi++)
            {
                /* if message needs to be reposted */
                if(nonprepost_need_repost[npi] && nonprepost_refcnt[rq->nonpp_md] == 0)
                {
                    /* repost the message slot */
                    nonprepost_repost(npi);
                }
            }
        }
        
        /* set the request to awaiting a user test */
	    rq->state = RQ_WAITING_USER_TEST;

	    goto foundout;
    }
    /* else, if this is a long message */
    else if(mb & match_bits_long_send)
    {
#if 0
        /* if this message has multiple buffers... */
        if (numbufs > 1) 
        {
	        /* need room for the iovec somewhere, might as well be here */
            struct bmip_work * rq2 = NULL;
            BMIP_WORK_ALLOC(rq2);

            /* make sure we could allocate the memory */
	        if (!rq2) 
            {
	            ret = -ENOMEM;
	            goto out;
	        }

            /* copy the request, free the orig request, and reset the orig request pointer */	
            BMIP_WORK_COPY(rq2, rq);
	        BMIP_WORK_FREE(rq);
	        rq = rq2;
        }
#endif
        /* setup the user buffer storage */
        if(rq->user_buffers == NULL)
        {
            /* copy a single value */
            if(numbufs < 2)
            {
                rq->user_buffers = *(char **)buffers;
            }
            /* copy the array of buffer pointers */
            else
            {
                rq->user_buffers = bmip_malloc(sizeof(void *) * numbufs);
                int n = 0;
                for(n = 0 ; n < numbufs ; n++)
                {
                    ((char**)rq->user_buffers)[n] = ((char **)buffers)[n];
                }
            }
        }
        if(rq->user_sizes == NULL)
        {
            /* copy a single value */
            if(numbufs < 2)
            {
                rq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t));
                *(rq->user_sizes) = *sizes;
            }
            /* copy all of the elements in the user size list to the request */
            else
            {
                rq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t) * numbufs);
                int n = 0;
                for(n = 0 ; n < numbufs ; n++)
                {
                    rq->user_sizes[n] = sizes[n];
                }
            }
        }
        if(rq->user_total_size == 0)
        {
            rq->user_total_size = total_size;
        }
        if(rq->user_numbufs == 0)
        {
            rq->user_numbufs = numbufs;
        }

        /* the md was already init... */
        if(rq->md != -1)
        {
            ret = PtlMDUpdate(rq->md, &mdesc, NULL, eq);
            if(ret)
            {
	            bmip_fprintf(stderr, "%s %i: PtlMDUpdate: %s\n", __func__, __LINE__, PtlErrorStr(ret));
            }

            /* update the MD desc */
            rq->tot_len = total_size;
            build_mdesc(rq, &mdesc, numbufs, buffers, sizes);
            mdesc.threshold = PTL_MD_THRESH_INF;  /* XXX: on Cray only, this must be 2, not 1 */

#if 0
        ret = PtlMDBind(ni, mdesc, PTL_UNLINK, &rq->md);
        if (ret)
        {
	        bmip_fprintf(stderr, "%s: PtlMDBind: %s\n", __func__, PtlErrorStr(ret));
	        ret = -EIO;
	        BMIP_WORK_FREE(rq);
	        goto out;
        }
        rq->manual_md_unlink = 1; 
#endif

            ret = PtlMDUpdate(rq->md, NULL, &mdesc, eq);
            if(ret)
            {
	            bmip_fprintf(stderr, "%s %i: PtlMDUpdate: %s\n", __func__, __LINE__, PtlErrorStr(ret));
            }
        }
        /* if the md was not yet init... insert me and attach md */
        else
        {
            build_mdesc_work(rq, &mdesc);
            mdesc.threshold = PTL_MD_THRESH_INF; /* send and reply */
            mdesc.options |= PTL_MD_OP_GET | PTL_MD_OP_PUT;

            /* create new match entry for this recv */
            ret = PtlMEInsert(mark_me, pma->pid, rq->match_bits, 0, PTL_UNLINK,
                PTL_INS_BEFORE, &rq->me);
            if (ret)
            {
                fprintf(stderr, "%s:%i PtlMEInsert: %s\n", __func__, __LINE__, PtlErrorStr(ret));
            }
            rq->manual_me_unlink = 1;

            /* create a new mem desciptor and attach the match entry to it */
            ret = PtlMDAttach(rq->me, mdesc, PTL_UNLINK, &rq->md);
            if (ret)
            {
                bmip_fprintf(stderr, "%s: PtlMDAttach: %s\n", __func__, PtlErrorStr(ret));
            }
            rq->manual_md_unlink = 1;
        }

        /* initiate the Get */
        ret = PtlGet(rq->md, pma->pid, ptl_index, 0, mb, 0);
        if (ret) 
        {
	        bmip_fprintf(stderr, "%s: PtlGet: %s\n", __func__, PtlErrorStr(ret));
	        ret = -EIO;
	        BMIP_WORK_FREE(rq);
	        goto out;
        }

        /* add this to the recv waiting get queue */
        rq->state = RQ_WAITING_GET;
        //bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&rq->list, &q_recv_waiting_get);
        //bmip_gen_mutex_unlock(&list_mutex);
        ret = 1; /* we handled it */
   
        goto out;
    }

foundout:
    /* move the request to the done request queue */
    bmip_gen_mutex_lock(&list_mutex);
    qlist_add_tail(&rq->list, &q_done);
    bmip_gen_mutex_unlock(&list_mutex);
    ret = 1;  /* we handled it */

out:
    return ret;
}

static int post_recv(bmi_op_id_t *id, struct bmi_method_addr *addr,
		     int numbufs, void *const *buffers, const bmi_size_t *sizes,
		     bmi_size_t total_size, bmi_msg_tag_t tag,
		     void *user_ptr, bmi_context_id context_id, uint32_t subseqno)
{
    struct bmip_method_addr *pma = addr->method_data;
    struct bmip_work *rq = NULL, * rqn = NULL, * srq = NULL;
    ptl_md_t mdesc;
    int ret = 0, ms = 0;
    uint64_t mb = 0;
    int found = 0;
    int i = 0;


    bmip_gen_mutex_lock(&eq_mutex);  /* do not let test threads manipulate eq */
    ret = ensure_ni_initialized(pma, any_pid);
    //bmip_gen_mutex_unlock(&eq_mutex);
    if (ret)
	goto out;

    /* increment the expected seqno of the message he will send us */
    if (++pma->seqno_in >= match_bits_seqno_max)
	pma->seqno_in = 0;

    rq = NULL;
    //bmip_gen_mutex_lock(&eq_mutex);  /* do not let test threads manipulate eq */
restart:
    
    /* if we can get the lock, try to drain the EQ */
    {
        __check_eq(ms);
    }

    /* first check the nonpreposted receive queue */
    ret = match_nonprepost_recv(id, addr, numbufs, buffers, sizes,
				total_size, tag, user_ptr, context_id, subseqno);

    /* match_nonprepost had an error or found / handled the request */
    if (ret != 0) 
    {
        /* it handled the request already */
	    if (ret > 0)  /* handled it via the nonprepost queue */
	        ret = 0;  /* reset this to 0 ro indicate no error */

	    goto out;  /* or error */
    }

    mb = mb_from_tag_and_seqno_and_subseqno(tag, pma->seqno_in, subseqno);

    /* check if we started a nonprepost transfer, but it has not yet completed */
    //bmip_gen_mutex_lock(&list_mutex);
    qlist_for_each_entry_safe(srq, rqn, &q_recv_nonprepost_start, list)
    {
        /* if the buffer request matches, mark as found and remove it from the list */
        if (srq->mop.addr == addr && srq->bmi_tag == tag && srq->match_bits == mb)
        {
            /* setup the user buffer storage */
            if(srq->user_buffers == NULL)
            {
                if(numbufs < 2)
                {
                    srq->user_buffers = *(char **)buffers;
                }
                else
                {
                    srq->user_buffers = bmip_malloc(sizeof(void *) * numbufs);
                    int n = 0;
                    for(n = 0 ; n < numbufs ; n++)
                    {
                        ((char**)srq->user_buffers)[n] = ((char **)buffers)[n];
                    }
                }
            }
            if(srq->user_sizes == NULL)
            {
                /* copy a single value */
                if(numbufs < 2)
                {
                    srq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t));
                    *(srq->user_sizes) = *sizes;
                }
                /* copy all of the elements in the user size list to the request */
                else
                {
                    srq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t) * numbufs);
                    int n = 0;
                    for(n = 0 ; n < numbufs ; n++)
                    {
                        srq->user_sizes[n] = sizes[n];
                    }
                } 
            }
            if(srq->user_total_size == 0)
            {
                srq->user_total_size = total_size;
            }
            if(srq->user_numbufs == 0)
            {
                srq->user_numbufs = numbufs;
            }

            fill_mop(srq, id, addr, user_ptr, context_id);

            /* update the MD desc */
            srq->tot_len = total_size;
            srq->actual_len = 0;
            srq->state = RQ_WAITING_PUT_END_GET_READY;

            /* we found the rq */
            found = 1;
            break;
        }
    }
   
    /* if the rq was found in the q_recv_nonprepost_start, add the updated version back to q_recv_nonprepost_start */ 
    if(found)
    {
        //qlist_add_tail(&rq->list, &q_recv_nonprepost_start);
        //bmip_gen_mutex_unlock(&list_mutex);
        srq = NULL;

        ret = 0;
        //goto partout;
        goto out;
    }
    else
    {
        //bmip_gen_mutex_unlock(&list_mutex);
    }
    //bmip_gen_mutex_unlock(&list_mutex);

    /* if the message size is less than or equal to the limit,
     *  let it be a nonprepost  wait incomming short
     */
    if(total_size <= NONPREPOST_MESSAGE_SIZE)
    {
        BMIP_WORK_ALLOC(rq);

        rq->type = BMI_RECV;
        rq->tot_len = total_size;
        rq->actual_len = 0;
        rq->bmi_tag = tag;
        fill_mop(rq, id, addr, user_ptr, context_id);

        mb = mb_from_tag_and_seqno_and_subseqno(tag, pma->seqno_in, subseqno);
        rq->match_bits = mb;

        /* setup the user buffer storage */
        if(rq->user_buffers == NULL)
        {
            if(numbufs < 2)
            {
                rq->user_buffers = *(char **)buffers;
            }
            else
            {
                rq->user_buffers = bmip_malloc(sizeof(void *) * numbufs);
                int n = 0;
                for(n = 0 ; n < numbufs ; n++)
                {
                    ((char**)rq->user_buffers)[n] = ((char **)buffers)[n];
                }
            }
        }
        if(rq->user_sizes == NULL)
        {
            if(numbufs < 2)
            {
                rq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t));
                *(rq->user_sizes) = *(sizes);
            }
            else
            {
                int uscount = 0;
                rq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t) * numbufs);
                for(uscount = 0 ; uscount < numbufs ; uscount++)
                {
                    rq->user_sizes[uscount] = sizes[uscount];
                }
            }
        }
        if(rq->user_total_size == 0)
        {
            rq->user_total_size = total_size;
        }
        if(rq->user_numbufs == 0)
        {
            rq->user_numbufs = numbufs;
        }

        /* add to nonprepost start list */
        rq->state = RQ_WAITING_PUT_END_GET_READY;
        //bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&rq->list, &q_recv_nonprepost_start);
        //bmip_gen_mutex_unlock(&list_mutex);
        ret = 0;

        /* go to the cleanup code */
        goto partout;
    }

#if 0
    /* if we found the recv on the start list */ 
    if(found != 0)
    {
        ret = 0;
        goto partout;
    }
    else
    {
        rq = NULL;
    }
#endif

    /* multiple trips through this loop caused by many MD_NO_UPDATEs, just
     * save the built-up rq and me/md from the first time through */
largemessage:
    /* if the request has not been setup */
    if(!rq)
    {
        /* allocate a new request */
	    BMIP_WORK_ALLOC(rq);
	    if (!rq)
        {
	        ret = -ENOMEM;
	        goto out;
	    }

        /* 
         * setup the recv request 
         *  - recv op
         *  - request size is the total size from the user
         *  - init actual recv len to 0
         *  - set the tag to the user tag
         *  - set the address
         *  - reset the mem descriptor and rebuild it
         *  - set the mem to inactive
         *  - allow put operations only
         */
	    rq->type = BMI_RECV;
	    rq->tot_len = total_size;
	    rq->actual_len = 0;
	    rq->bmi_tag = tag;
	    fill_mop(rq, id, addr, user_ptr, context_id);
	    build_mdesc(rq, &mdesc, numbufs, buffers, sizes);
	    mdesc.threshold = 0;  /* initially inactive */
	    mdesc.options |= PTL_MD_OP_PUT;

	    /* put at the end of the preposted list, just before the first
	     * nonprepost or unex ME. */
	    rq->me = PTL_INVALID_HANDLE;
	    mb = mb_from_tag_and_seqno_and_subseqno(tag, pma->seqno_in, subseqno);
        rq->match_bits = mb;

        if(total_size > NONPREPOST_MESSAGE_SIZE)
        {
            rq->match_bits |= match_bits_long_send;
        }

        /* setup the user buffer storage */
        if(rq->user_buffers == NULL)
        {
            if(numbufs < 2)
            {
                rq->user_buffers = *(char **)buffers;
            }
            else
            {
                rq->user_buffers = bmip_malloc(sizeof(void *) * numbufs);
                int n = 0;
                for(n = 0 ; n < numbufs ; n++)
                {
                    ((char**)rq->user_buffers)[n] = ((char **)buffers)[n];
                }
            }
        }
        if(rq->user_sizes == NULL)
        {
            if(numbufs < 2)
            {
                rq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t));
                *(rq->user_sizes) = *(sizes);
            }
            else
            {
                int uscount = 0;
                rq->user_sizes = (bmi_size_t *)bmip_malloc(sizeof(bmi_size_t) * numbufs);
                for(uscount = 0 ; uscount < numbufs ; uscount++)
                {
                    rq->user_sizes[uscount] = sizes[uscount];
                }
            }
        }
        if(rq->user_total_size == 0)
        {
            rq->user_total_size = total_size;
        }
        if(rq->user_numbufs == 0)
        {
            rq->user_numbufs = numbufs;
        }

        /* create new match entry for this recv */
        //fprintf(stderr, "%s:%i insert ME\n", __func__, __LINE__);
	    ret = PtlMEInsert(mark_me, pma->pid, mb, 0, PTL_UNLINK,
			  PTL_INS_BEFORE, &rq->me);
	    if (ret)
        {
	        fprintf(stderr, "%s:%i PtlMEInsert: %s\n", __func__, __LINE__, PtlErrorStr(ret));
	        ret = -EIO;
	        goto out;
	    }
        rq->manual_me_unlink = 1;

        /* create a new mem desciptor and attach the match entry to it */
	    ret = PtlMDAttach(rq->me, mdesc, PTL_UNLINK, &rq->md);
	    if (ret)
        {
	        bmip_fprintf(stderr, "%s: PtlMDAttach: %s\n", __func__, PtlErrorStr(ret));
	        ret = -EIO;
	        goto out;
	    }
        rq->manual_md_unlink = 1;
    }

    /* now update it atomically with respect to the event stream from the NIC */
    //mdesc.threshold = 1;
    mdesc.threshold = PTL_MD_THRESH_INF;
    ret = PtlMDUpdate(rq->md, NULL, &mdesc, eq);

    /* if the update was not succesful */
    if (ret) 
    {
        /* the update was not performed, restart the op */
	    if (ret == PTL_MD_NO_UPDATE)
        {
	        /* cannot block, other thread may have processed the event for us */
            if(ms == 0)
                ms = 0;
            else
                ms *= 2;
	        goto restart;
	    }
        /* another error occured... exit */
        else
        {
	        bmip_fprintf(stderr, "%s: PtlMDUpdate: %s\n", __func__, PtlErrorStr(ret));
	        ret = -EIO;
	        goto out;
        }
    }

    /* add the rquest to the waiting incomming queue */
    rq->state = RQ_WAITING_INCOMING;
    //bmip_gen_mutex_lock(&list_mutex);
    qlist_add_tail(&rq->list, &q_recv_waiting_incoming);
    //bmip_gen_mutex_unlock(&list_mutex);

    /* keep the rq allocated since it is on the waiting incomming list */
    rq = NULL;

out:
    /* if we created a request */
    if(rq) 
    {
	    /*
	    * Alloced, then found MD_NO_UPDATE, and it had completed on the
	    * unexpected.  Free this temp rq.  (Or error case too.)
	    */
        
        /* if we have a valid match enrty, unlink it */
	    if (rq->me != PTL_INVALID_HANDLE)
        {
	        int pret = PtlMEUnlink(rq->me);
            if(pret)
            {
	            fprintf(stderr, "%s:%i PtlMEUnlink: %s\n", __func__, __LINE__, PtlErrorStr(ret));
            }
        }

        /* free the request we allocated */
        rq->manual_me_unlink = 0;
        rq->manual_md_unlink = 0;
	    BMIP_WORK_FREE(rq);
    }
partout:
    bmip_gen_mutex_unlock(&eq_mutex);
    return ret;
}

static int bmip_post_recv(bmi_op_id_t *id, struct bmi_method_addr *remote_map,
			  void *buffer, bmi_size_t expected_len,
			  bmi_size_t *actual_len __unused,
			  enum bmi_buffer_type buffer_flag __unused,
			  bmi_msg_tag_t tag, void *user_ptr,
			  bmi_context_id context_id)
{
    int ret = 0;
    struct bmip_multi_work * mpw = NULL, * mpwn = NULL;
    QLIST_HEAD(q_recv_list);

#ifndef BMIPTLSEG
        BMIP_WORK_MULTI_ALLOC(mpw);
        ret = post_recv(id, remote_map, 1, &buffer, &expected_len,
            expected_len, tag, user_ptr, context_id, 0);

        /* setup multi part work unit */
        mpw->id = *id;
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->accum_bytes = expected_len;
        mpw->user_ptr = user_ptr;

        bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
        bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);

        return ret;
#else
    bmip_gen_mutex_lock(&op_mutex);

    if(expected_len <= BMIP_MAX_SEGMENT_SIZE)
    {
        BMIP_WORK_MULTI_ALLOC(mpw);
        ret = post_recv(id, remote_map, 1, &buffer, &expected_len,
	        expected_len, tag, user_ptr, context_id, 0);

        /* setup multi part work unit */
        mpw->id = *id;
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->accum_bytes = expected_len;
        mpw->user_ptr = user_ptr;

        bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
        bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);

        return ret;
    }
    else
    {
        bmi_size_t tbytes = 0;
        void * cur_buffer = NULL;
        bmi_size_t cur_off = 0;
        bmi_size_t cur_size = 0;
        bmi_size_t accum_bytes = 0;
        int is_last = 0;
        struct bmip_multi_work * mpw = NULL;
        uint32_t subindex = 0;

        while(tbytes < expected_len)
        {
            if(expected_len - tbytes >= BMIP_MAX_SEGMENT_SIZE)
            {
                cur_size = BMIP_MAX_SEGMENT_SIZE;
                accum_bytes += cur_size;
            }
            else
            {
                cur_size = expected_len - tbytes;
            }

            /* update the transfer bytes count */
            tbytes += cur_size;

            /* check if this is the last transfer */
            if(tbytes == expected_len)
                is_last = 1;

            /* update the buffer */
            cur_buffer = buffer + cur_off;
            cur_off += BMIP_MAX_SEGMENT_SIZE;

            /* recv the buffer */
            if(!is_last)
            {
                BMIP_WORK_MULTI_ALLOC(mpw);

                ret = post_recv(&mpw->id, remote_map, 1, &cur_buffer, &cur_size,
                    cur_size, tag, NULL, context_id, subindex);

                /* setup multi part work unit */
                mpw->tag = tag;
                mpw->index = subindex;
                mpw->accum_bytes = accum_bytes;
                mpw->user_ptr = NULL;
	            bmip_gen_mutex_lock(&list_mutex);
                qlist_add_tail(&mpw->list, &q_recv_list);
	            bmip_gen_mutex_unlock(&list_mutex);
                mpw = NULL;
            }
            else
            {
                ret = post_recv(id, remote_map, 1, &cur_buffer, &cur_size,
                    cur_size, tag, user_ptr, context_id, subindex);

                /* setup multi part work unit */
                BMIP_WORK_MULTI_ALLOC(mpw);
                mpw->id = *id;
                mpw->index = subindex;
                mpw->user_ptr = user_ptr;
                mpw->accum_bytes = accum_bytes;
	            bmip_gen_mutex_lock(&list_mutex);
                qlist_add_tail(&mpw->list, &q_recv_list);
	            bmip_gen_mutex_unlock(&list_mutex);
            }
            subindex++;
        }

        /* transfer the local list to the master list */
        bmip_gen_mutex_lock(&list_mutex);
        qlist_for_each_entry_safe(mpw, mpwn, &q_recv_list, list)
        {
            /* set the segment total */
            mpw->total = subindex;

            /* set the master op id */
            mpw->master_id = *id;

            mpw->user_ptr = user_ptr;

            /* remove from the local list */
            qlist_del(&(mpw->list));

            /* add it to the master list */
            qlist_add_tail(&mpw->list, &q_mp_done);
        }

        struct bmip_master_mp * m = NULL;
        BMIP_WORK_MULTI_MASTER_ALLOC(m);
        m->master_id = *id;
        m->count = subindex; 
        qlist_add_tail(&m->list, &q_master_mp);
        bmip_gen_mutex_unlock(&list_mutex);
    }
    bmip_gen_mutex_unlock(&op_mutex);
    return ret;
#endif
}

static int bmip_post_recv_list(bmi_op_id_t *id, struct bmi_method_addr *remote_map,
			       void *const *buffers, const bmi_size_t *sizes,
			       int list_count, bmi_size_t tot_expected_len,
			       bmi_size_t *tot_actual_len __unused,
			       enum bmi_buffer_type buffer_flag __unused,
			       bmi_msg_tag_t tag, void *user_ptr,
			       bmi_context_id context_id)
{
    int ret = 0;
    struct bmip_multi_work * mpw = NULL, * mpwn = NULL;
    QLIST_HEAD(q_recv_list);

#ifndef BMIPTLSEG
        BMIP_WORK_MULTI_ALLOC(mpw);

        /* setup multi part work unit */
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->user_ptr = user_ptr;
        mpw->accum_bytes = tot_expected_len;

        ret = post_recv(id, remote_map, list_count, buffers, sizes,
             tot_expected_len, tag, user_ptr, context_id, 0);

        mpw->id = *id;

        bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
        bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);
        return ret;
#else
    bmip_gen_mutex_lock(&op_mutex);
    if(tot_expected_len <= BMIP_MAX_SEGMENT_SIZE)
    {
        BMIP_WORK_MULTI_ALLOC(mpw);

        /* setup multi part work unit */
        mpw->tag = tag;
        mpw->index = 0;
        mpw->total = 1;
        mpw->user_ptr = user_ptr;
        mpw->accum_bytes = tot_expected_len;

        ret = post_recv(id, remote_map, list_count, buffers, sizes,
		     tot_expected_len, tag, user_ptr, context_id, 0);

        mpw->id = *id;

        bmip_gen_mutex_lock(&list_mutex);
        qlist_add_tail(&mpw->list, &q_mp_done);
        bmip_gen_mutex_unlock(&list_mutex);

        bmip_gen_mutex_unlock(&op_mutex);
        return ret;
    }
    else
    {
        int lcount = 0;
        bmi_size_t accum_bytes = 0;
        uint32_t subindex = 0;
    
        /* local list recv list */
        for(lcount = 0 ; lcount < list_count ; lcount++)
        {
            bmi_size_t tbytes = 0;
            void * cur_buffer = NULL;
            bmi_size_t cur_off = 0;
            bmi_size_t cur_size = 0;
            int is_last = 0;

            while(tbytes < sizes[lcount])
            {
                if(sizes[lcount] - tbytes >= BMIP_MAX_SEGMENT_SIZE)
                {
                    cur_size = BMIP_MAX_SEGMENT_SIZE;
                    accum_bytes += cur_size;
                }
                else
                {
                    cur_size = sizes[lcount] - tbytes;
                    if(tbytes == 0)
                        accum_bytes += cur_size;
                }

                /* update the transfer bytes count */
                tbytes += cur_size;

                /* check if this is the last transfer */
                if(tbytes == sizes[lcount] && lcount + 1 == list_count)
                    is_last = 1;

                /* update the buffer */
                cur_buffer = buffers[lcount] + cur_off;
                cur_off += BMIP_MAX_SEGMENT_SIZE;

                /* recv the buffer */
                if(!is_last)
                {
                    /* set the user_ptr to NULL and do not use the master ID */
                    BMIP_WORK_MULTI_ALLOC(mpw);

                    /* setup multi part work unit */
                    mpw->tag = tag;
                    mpw->index = subindex;
                    mpw->user_ptr = user_ptr;
                    mpw->accum_bytes = accum_bytes;

                    ret = post_recv(&mpw->id, remote_map, 1, &cur_buffer, &cur_size,
                        cur_size, tag, NULL, context_id, subindex);
        
                    bmip_gen_mutex_lock(&list_mutex);
                    qlist_add_tail(&mpw->list, &q_recv_list);
                    bmip_gen_mutex_unlock(&list_mutex);

                    mpw = NULL;
                }
                else
                {
                    /* use the master ID and user_ptr */
                    BMIP_WORK_MULTI_ALLOC(mpw);

                    /* setup multi part work unit */
                    mpw->tag = tag;
                    mpw->user_ptr = user_ptr;
                    mpw->index = subindex;
                    mpw->accum_bytes = accum_bytes;

                    ret = post_recv(id, remote_map, 1, &cur_buffer, &cur_size,
                        cur_size, tag, user_ptr, context_id, subindex);

                    mpw->id = *id;

                    bmip_gen_mutex_lock(&list_mutex);
                    qlist_add_tail(&mpw->list, &q_recv_list);
                    bmip_gen_mutex_unlock(&list_mutex);
                }
    
                /* inc the subindex */
                subindex++;
            } /* while */
        } /* for */

        /* transfer the local list to the master list */
        bmip_gen_mutex_lock(&list_mutex);
        qlist_for_each_entry_safe(mpw, mpwn, &q_recv_list, list)
        {
            /* set the segment total */
            mpw->total = subindex;

            /* set the master op id */
            mpw->master_id = *id;

            mpw->user_ptr = user_ptr;

            /* remove from the local list */
            qlist_del(&(mpw->list));

            /* add it to the master list */
            qlist_add_tail(&mpw->list, &q_mp_done);
        }
        bmip_gen_mutex_unlock(&list_mutex);

        struct bmip_master_mp * m = NULL;
        BMIP_WORK_MULTI_MASTER_ALLOC(m);
        m->master_id = *id;
        m->count = subindex;
        qlist_add_tail(&m->list, &q_master_mp);
    } /* if else */

    bmip_gen_mutex_unlock(&op_mutex);
    return ret;
#endif
}

#if 0
                /* wait for each transfer except the last */
                if(!is_last)
                {
                    bmi_error_code_t err;
                    struct bmip_work * w = NULL, * wn = NULL;
                    int n = 0;

                    /* check for the comp of the send */
                    int timeout = 1;
                    bmip_gen_mutex_lock(&eq_mutex);
                    for (;;)
                    {
                        fprintf(stderr, "%s:%i timeout = %i, subid = %i\n", __func__, __LINE__, timeout, subid);
                        ret = __check_eq(timeout);
                        //ret = check_eq(timeout);
                        if (ret)
                            break;

                        /* search for id */
                        bmip_gen_mutex_lock(&list_mutex);
                        qlist_for_each_entry_safe(w, wn, &q_done, list)
                        {
                            fprintf(stderr, "%s:%i w->mop.op_id = %i, subid = %i\n", __func__, __LINE__, w->mop.op_id, subid);
                            if (w->mop.op_id == subid)
                            {
                                fill_done(w, &subid, &cur_size, NULL, &err);
                                ++n;
                                qlist_del(&(w->list));
                                BMIP_WORK_FREE(w);
                                break;
                            }
                        }
                        bmip_gen_mutex_unlock(&list_mutex);

                        if (n > 0)
                            break;
                        else
                            timeout *= 2;
                    }
                    bmip_gen_mutex_unlock(&eq_mutex);
                }
            }
        }
    }
    return ret;
}
#endif

/* debugging */
#define show_queue(q) do { \
    fprintf(stderr, #q "\n"); \
    qlist_for_each_entry(w, &q, list) { \
	fprintf(stderr, "%s %p state %s len %llu actual len %llu tag %i mb %0llx\n", \
		w->type == BMI_SEND ? "sq" : "rq", \
		w, state_name(w->state), \
		llu(w->tot_len), llu(w->actual_len), \
		llu(w->bmi_tag), llu(w->match_bits)); \
    } \
} while (0)

static void dump_queues(int sig __unused)
{
    struct bmip_work *w;

    /* debugging */
    show_queue(q_send_waiting_ack);
    show_queue(q_send_waiting_get);
    show_queue(q_recv_waiting_incoming);
    show_queue(q_recv_waiting_get);
    show_queue(q_recv_waiting_put_end);
    show_queue(q_recv_nonprepost);
    show_queue(q_recv_nonprepost_start);
    show_queue(q_unexpected_done);
    show_queue(q_done);
}

/*
 * Cancel.  Grab the eq lock to keep things from finishing as we are
 * freeing them.  Hopefully this won't lead to core dumps in future
 * test operations.
 */
static int bmip_cancel(bmi_op_id_t id, bmi_context_id context_id __unused)
{
    int ret = 0;
    struct method_op *mop;
    struct bmip_work *w;

    //bmip_gen_mutex_lock(&list_mutex);
    bmip_gen_mutex_lock(&eq_mutex);
    __check_eq(0);
    mop = id_gen_fast_lookup(id);
    w = mop->method_data;
    bmip_fprintf(stderr, "%s: cancel %p state %s len %llu tag 0x%llx mb 0x%llx\n",
    	    __func__, w, state_name(w->state), llu(w->tot_len),
	    llu(w->bmi_tag), llu(w->match_bits));
    switch (w->state) {

    case SQ_WAITING_ACK:
	w->state = SQ_CANCELLED;
	goto link_done;

    case SQ_WAITING_GET:
	w->state = SQ_CANCELLED;
	ret = PtlMEUnlink(w->me);
	if (ret)
	    gossip_err("%s: PtlMEUnlink: %s\n", __func__, PtlErrorStr(ret));
	goto link_done;

    case RQ_WAITING_INCOMING:
	w->state = RQ_CANCELLED;
	ret = PtlMEUnlink(w->me);
	if (ret)
	    gossip_err("%s: PtlMEUnlink: %s\n", __func__, PtlErrorStr(ret));
	goto link_done;

    case RQ_WAITING_PUT_END:
	w->state = RQ_CANCELLED;
	ret = PtlMEUnlink(w->me);
	if (ret)
	    gossip_err("%s: PtlMEUnlink: %s\n", __func__, PtlErrorStr(ret));
	goto link_done;

    case RQ_WAITING_GET:
    {
	    w->state = RQ_CANCELLED;
        bmip_fprintf(stderr, "%s: unlink md\n", __func__);
	    ret = PtlMDUnlink(w->md);
	    if (ret)
	        /* complain, but might be okay if we raced with the completion */
	        bmip_fprintf(stderr, "%s: PtlMDUnlink: %s\n", __func__, PtlErrorStr(ret));
	    goto link_done;
    }

    case SQ_WAITING_USER_TEST:
    case RQ_WAITING_USER_TEST:
    case RQ_WAITING_USER_POST:
    case RQ_LEN_ERROR:
    case SQ_CANCELLED:
    case RQ_CANCELLED:
	/* nothing to do */
	break;
    }
    goto out;

link_done:
    bmip_gen_mutex_lock(&list_mutex);
    qlist_del(&w->list);
    qlist_add_tail(&w->list, &q_done);
    bmip_gen_mutex_unlock(&list_mutex);

out:
    bmip_gen_mutex_unlock(&eq_mutex);

    /* debugging */
    dump_queues(0);

    exit(1);
    return 0;
}

static const char *bmip_rev_lookup(struct bmi_method_addr *addr)
{
    struct bmip_method_addr *pma = addr->method_data;

    return pma->peername;
}

/*
 * Build and fill a Portals-specific method_addr structure.  This routine
 * copies the hostname if it needs it.
 */
static struct bmi_method_addr *bmip_alloc_method_addr(const char *hostname,
						  ptl_process_id_t pid,
						  int register_with_bmi)
{
    struct bmi_method_addr *map;
    struct bmip_method_addr *pma, * pman;
    bmi_size_t extra;
    int ret = 0;

    /*
     * First search to see if this one already exists.
     */
    bmip_gen_mutex_lock(&pma_mutex);
    qlist_for_each_entry_safe(pma, pman, &pma_list, list) {
	if (pma->pid.pid == pid.pid && pma->pid.nid == pid.nid) {
	    /* relies on alloc_method_addr() working like it does */
	    map = &((struct bmi_method_addr *) pma)[-1];
	    //debug(2, "%s: found matching peer %s\n", __func__, pma->peername);
	    goto out;
	}
    }
    bmip_gen_mutex_unlock(&pma_mutex);

    /* room for a peername tacked on the end too, no more than 10 digits */
    extra = sizeof(*pma) + 2 * (strlen(hostname) + 1) + 10 + 1;

    map = bmi_alloc_method_addr(bmi_portals_method_id, extra);
    pma = map->method_data;

    pma->hostname = (void *) &pma[1];
    pma->peername = pma->hostname + strlen(hostname) + 1;
    strcpy(pma->hostname, hostname);
    /* for debug/error messages via BMI_addr_rev_lookup */
    sprintf(pma->peername, "%s:%d", hostname, pid.pid);

    pma->pid.pid = pid.pid;
    pma->pid.nid = pid.nid;
    pma->seqno_in = 0;
    pma->seqno_out = 0;
    bmip_gen_mutex_lock(&pma_mutex);
    qlist_add_tail(&pma->list, &pma_list);
    bmip_gen_mutex_unlock(&pma_mutex);

    if (register_with_bmi) {
	ret = bmi_method_addr_reg_callback(map);
	if (!ret) {
	    gossip_err("%s: bmi_method_addr_reg_callback failed\n", __func__);
	    bmip_free(map);
	    map = NULL;
	}
    }

    return map;

out:
    bmip_gen_mutex_unlock(&pma_mutex);
    return map;
}


#if !(defined(__LIBCATAMOUNT__) || defined(__CRAYXT_COMPUTE_LINUX_TARGET) || defined(__CRAYXT_SERVICE))
/*
 * Clients give hostnames.  Convert these to Portals nids.  This routine
 * specific for Portals-over-IP (tcp or utcp).
 */
static int bmip_nid_from_hostname(const char *hostname, uint32_t *nid)
{
    struct hostent *he;

    he = gethostbyname(hostname);
    if (!he) {
	gossip_err("%s: gethostbyname cannot resolve %s\n", __func__, hostname);
	return 1;
    }
    if (he->h_length != 4) {
	gossip_err("%s: gethostbyname returns %d-byte addresses, hoped for 4\n",
		   __func__, he->h_length);
	return 1;
    }
    /* portals wants host byte order, apparently */
    *nid = htonl(*(uint32_t *) he->h_addr_list[0]);
    return 0;
}

/*
 * This is called from the server, on seeing an unexpected message from
 * a client.  Convert that to a method_addr.  If BMI has never seen it
 * before, register it with BMI.
 *
 * Ugh, since Portals is connectionless, we need to search through
 * all the struct bmip_method_addr to find the pid that sent
 * this to us.  While BMI has a list of all method_addrs somewhere,
 * it is not exported to us.  So we have to maintain our own list
 * structure.
 */
static struct bmi_method_addr *addr_from_nidpid(ptl_process_id_t pid)
{
    struct bmi_method_addr *map;
    struct in_addr inaddr;
    char *hostname;

    /* temporary, for ip 100.200.200.100 */
    hostname = bmip_malloc(INET_ADDRSTRLEN + 1);
    inaddr.s_addr = htonl(pid.nid);  /* ntop expects network format */
    inet_ntop(AF_INET, &inaddr, hostname, INET_ADDRSTRLEN);

    map = bmip_alloc_method_addr(hostname, pid, 1);
    bmip_free(hostname);

    return map;
}

#else

/*
 * Cray versions
 */
static int bmip_nid_from_hostname(const char *hostname, uint32_t *nid)
{
    int ret = -1;
    uint32_t v = 0;
    char *cp;

    /*
     * There is apparently no API for this on Cray.  Chop up the hostname,
     * knowing the format.  Also can look at /proc/cray_xt/nid on linux
     * nodes.
     */
    if (strncmp(hostname, "nid", 3) == 0) {
	v = strtoul(hostname + 3, &cp, 10);
	if (*cp != '\0') {
	    gossip_err("%s: convert nid<num> hostname %s failed\n", __func__,
		       hostname);
		//       hostname);
	    v = 0;
	} else {
	    ret = 0;
	}
    }
    if (ret)
    {
	//debug(0, "%s: no clue how to convert hostname %s\n", __func__,
	  //    hostname);
	//    hostname);
    }
    *nid = v;
    return ret;
}

static struct bmi_method_addr *addr_from_nidpid(ptl_process_id_t pid)
{
    struct bmi_method_addr *map;
    char hostname[9];

    sprintf(hostname, "nid%05d", pid.nid);
    map = bmip_alloc_method_addr(hostname, pid, 1);
    return map;
}
#endif

/*
 * Break up a method string like:
 *   portals://hostname:pid/filesystem
 * into its constituent fields, storing them in an opaque
 * type, which is then returned.
 */
static struct bmi_method_addr *bmip_method_addr_lookup(const char *id)
{
    char *s, *cp, *cq;
    int ret = 0;
    ptl_process_id_t pid;
    struct bmi_method_addr *map = NULL;

    /* parse hostname */
    s = string_key("portals", id);  /* allocs a string "node27:3334/pvfs2-fs" */
    if (!s)
	return NULL;
    cp = strchr(s, ':');
    if (!cp) {
	gossip_err("%s: no ':' found\n", __func__);
	goto out;
    }

    /* terminate hostname from s to cp */
    *cp = 0;
    ++cp;

    /* strip /filesystem part */
    cq = strchr(cp, '/');
    if (cq)
	*cq = 0;

    /* parse pid part */
    pid.pid = strtoul(cp, &cq, 10);
    if (cq == cp) {
	gossip_err("%s: invalid pid number\n", __func__);
	goto out;
    }
    if (*cq != '\0') {
	gossip_err("%s: extra characters after pid number\n", __func__);
	goto out;
    }

    ret = bmip_nid_from_hostname(s, &pid.nid);
    if (ret)
	goto out;

    /*
     * Lookup old one or Alloc new one, but don't call
     * bmi_method_addr_reg_callback---this is the client side, BMI is asking us
     * to look this up and will register it itself.
     */
    map = bmip_alloc_method_addr(s, pid, 0);

out:
    bmip_free(s);
    return map;
}

/*
 * "Listen" on a particular portal, specified in the address in pvfs2tab.
 */
static int unexpected_init(struct bmi_method_addr *listen_addr)
{
    struct bmip_method_addr *pma = NULL;
    int i, ret = 0;
    if(listen_addr)
        pma = listen_addr->method_data;

    unexpected_buf = bmip_malloc(UNEXPECTED_QUEUE_SIZE);
    if (!unexpected_buf) {
	ret = -ENOMEM;
	goto out;
    }

    if(pma)
    {
        bmip_gen_mutex_lock(&eq_mutex);
        ret = ensure_ni_initialized(pma, pma->pid);
        bmip_gen_mutex_unlock(&eq_mutex);
        if (ret)
	        goto out;
    }
    else
    {   
        bmip_gen_mutex_lock(&eq_mutex);
        ret = ensure_ni_initialized(NULL, any_pid);
        bmip_gen_mutex_unlock(&eq_mutex);
        if (ret)
	        goto out;
    }

    /*
     * We use two, half-size, MDs.  When one fills up, it is unlinked and we
     * find out about it via an event.  The second one is used to give us time
     * to repost the first.  Sort of a circular buffer structure.  This is
     * hopefully better than wasting a full 8k for every small control message.
     */
    unexpected_need_repost_sum = 0;
    for (i=0; i<UNEXPECTED_NUM_MD; i++) {
	unexpected_is_posted[i] = 0;
	unexpected_need_repost[i] = 0;
	unexpected_repost(i);
    }

out:
    return ret;
}

static void unexpected_repost(int which)
{
    int ret = 0;
    ptl_md_t mdesc;

    /* unlink used-up one */
    if (unexpected_is_posted[which]) 
    {
	    ret = PtlMEUnlink(unexpected_me[which]);
	    if (ret) 
        {
	        bmip_fprintf(stderr, "%s: PtlMEUnlink %d: %s\n", __func__, which, PtlErrorStr(ret));
	        return;
	    }
	    unexpected_need_repost[which] = 0;
	    unexpected_is_posted[which] = 0;
	    --unexpected_need_repost_sum;
    }

    /* unexpected messages are limited by the API to a certain size */
    mdesc.start = unexpected_buf + which * (UNEXPECTED_QUEUE_SIZE / 2);
    mdesc.length = UNEXPECTED_QUEUE_SIZE / 2;
    mdesc.threshold = PTL_MD_THRESH_INF;
    mdesc.options = PTL_MD_OP_PUT | PTL_MD_MAX_SIZE;
    mdesc.max_size = UNEXPECTED_MESSAGE_SIZE;
    mdesc.eq_handle = eq;
    mdesc.user_ptr = (void *) (uintptr_t) (UNEXPECTED_MD_INDEX_OFFSET + which);

    /*
     * Take any tag, as long as it has the unexpected bit set, and not
     * the long send bit.  Not sure if we need both bits for this.  This always
     * goes at the very end of the list, just in front of zero.  The nonpp
     * and unex ones can be comingled, as they select different things, but
     * they must come after the preposts and before the zero md.
     */
    ret = PtlMEInsert(zero_me, any_pid, match_bits_unexpected,
		      (0x3fffffffULL << 32) | 0xffffffffULL, PTL_UNLINK,
		      PTL_INS_BEFORE, &unexpected_me[which]);
    if (ret)
    {
	    bmip_fprintf(stderr, "%s: PtlMEInsert: %s\n", __func__, PtlErrorStr(ret));
	    return;
    }

    /*
     * Put data here when it matches.  Do not auto-unlink else a new md
     * may get stuck in and cause a false match in unexpected_md_index above.
     * Do it all manually.  Also have to make sure these do not get reused
     * in case things sitting in the queue haven't been looked at yet.  Maybe
     * need to use md.user_ptr, or look at the match bits.
     */
    ret = PtlMDAttach(unexpected_me[which], mdesc, PTL_RETAIN,
		      &unexpected_md[which]);
    if (ret)
	    bmip_fprintf(stderr, "%s: PtlMDAttach: %s\n", __func__, PtlErrorStr(ret));

    unexpected_is_posted[which] = 1;
}

static int unexpected_fini(void)
{
    int i, ret = 0;

    for (i=0; i<UNEXPECTED_NUM_MD; i++) {
	/* MDs go away when MEs unlinked */
	ret = PtlMEUnlink(unexpected_me[i]);
	if (ret) {
	    gossip_err("%s: PtlMEUnlink %d: %s\n", __func__, i,
		       PtlErrorStr(ret));
	    return ret;
	}
    }
    bmip_free(unexpected_buf);
    return 0;
}

/*
 * Manage nonprepost buffers.
 */
static int nonprepost_init(void)
{
    int i, ret = 0;

    nonprepost_buf = bmip_malloc(NONPREPOST_QUEUE_SIZE);
    if (!nonprepost_buf) {
	ret = -ENOMEM;
	goto out;
    }

    /*
     * See comments above for unexpected.
     */
    nonprepost_need_repost_sum = 0;
    for (i=0; i<NONPREPOST_NUM_MD; i++) 
    {
	    nonprepost_is_posted[i] = 0;
	    nonprepost_refcnt[i] = 0;
        nonprepost_need_repost[i] = 0;
	    nonprepost_repost(i);
    }

out:
    return ret;
}

static void nonprepost_repost(int which)
{
    int ret = 0;
    ptl_md_t mdesc;
#if 0
    static int count = 0;

    /* update the count */
    ++count;
    if (count > 2)
    {
	    exit(0);
    }
#endif

    /* unlink used-up one */
    if(nonprepost_is_posted[which])
    {
	    ret = PtlMEUnlink(nonprepost_me[which]);
	    if(ret) 
        {
	        bmip_fprintf(stderr, "%s: PtlMEUnlink %d: %s\n", __func__, which,
		        PtlErrorStr(ret));
	        return;
	    }
        
        /* update the unexpected repost data */
	    nonprepost_need_repost[which] = 0;
	    nonprepost_is_posted[which] = 0;
	    --nonprepost_need_repost_sum;
    }

    /* only short messages that fit max_size go in here */
    mdesc.start = nonprepost_buf + which * (NONPREPOST_QUEUE_SIZE / 2);
    mdesc.length = NONPREPOST_QUEUE_SIZE / 2;
    mdesc.threshold = PTL_MD_THRESH_INF;
    mdesc.options = PTL_MD_OP_PUT | PTL_MD_MAX_SIZE;
    mdesc.max_size = NONPREPOST_MESSAGE_SIZE;
    mdesc.eq_handle = eq;
    mdesc.user_ptr = (void *) (uintptr_t) (NONPREPOST_MD_INDEX_OFFSET + which);

    /* XXX: maybe need manual unlink like for unexpecteds on CNL */

    /* also at the very end of the list */
    /* match anything as long as top two bits are zero */
    ret = PtlMEInsert(zero_me, any_pid, 0,
		      (0x3fffffffULL << 32) | 0xffffffffULL,
		      PTL_UNLINK, PTL_INS_BEFORE, &nonprepost_me[which]);
    if (ret) {
	gossip_err("%s: PtlMEInsert: %s\n", __func__, PtlErrorStr(ret));
	return;
    }

    /* put data here when it matches; when full, unlink it */
    ret = PtlMDAttach(nonprepost_me[which], mdesc, PTL_UNLINK,
		      &nonprepost_md[which]);
    if (ret) {
	gossip_err("%s: PtlMDAttach: %s\n", __func__, PtlErrorStr(ret));


	return;
    }

    nonprepost_is_posted[which] = 1;
}

static int nonprepost_fini(void)
{
    int i, ret = 0;

    for (i=0; i<NONPREPOST_NUM_MD; i++) 
    {
	    if (nonprepost_refcnt[i] != 0)
	        gossip_err("%s: refcnt %d, should be zero\n", __func__,
		       nonprepost_refcnt[i]);
	    if (!nonprepost_is_posted[i])
        {
	        continue;
        }

	    /* MDs go away when MEs unlinked */
	    ret = PtlMEUnlink(nonprepost_me[i]);
	    if (ret) 
        {
	        gossip_err("%s: PtlMEUnlink %d: %s\n", __func__, i,
		       PtlErrorStr(ret));
	    }
    }
    bmip_free(nonprepost_buf);
    return 0;
}


static void *bmip_memalloc(bmi_size_t len, enum bmi_op_type send_recv __unused)
{
#if 1
    void * ptr = NULL;
    {
        if(posix_memalign(&ptr, BMIP_ALIGN, len))
        {
            return NULL;
        }
    }
    return ptr;
#else
    if(len < BMIP_ALIGN)
    {
        len = BMIP_ALIGN;
    }
    return bmip_malloc(len);
#endif
}

static int bmip_memfree(void *buf, bmi_size_t len __unused,
			enum bmi_op_type send_recv __unused)
{
    bmip_free(buf);
    return 0;
}

static int bmip_unexpected_free(void *buf)
{
    bmip_free(buf);
    return 0;
}

/*
 * No need to track these internally.  Just search the entire queue.
 */
static int bmip_open_context(bmi_context_id context_id __unused)
{
    return 0;
}

static void bmip_close_context(bmi_context_id context_id __unused)
{
}

/*
 * Callers sometimes want to know odd pieces of information.  Satisfy
 * them.
 */
static int bmip_get_info(int option, void *param)
{
    int ret = 0;

    switch (option) {
	case BMI_CHECK_MAXSIZE:
	    /* reality is 2^31, but shrink to avoid negative int */
	    *(int *)param = (1UL << 31) - 1;
	    break;
	case BMI_GET_UNEXP_SIZE:
	    *(int *)param = UNEXPECTED_MESSAGE_SIZE;
	    break;
	default:
	    ret = -ENOSYS;
    }
    return ret;
}

/*
 * Used to set some optional parameters and random functions, like ioctl.
 */
static int bmip_set_info(int option, void *param)
{
    switch (option) {
    case BMI_DROP_ADDR: {
	struct bmi_method_addr *addr = param;
	struct bmip_method_addr *pma = addr->method_data;
	qlist_del(&pma->list);
	bmip_free(addr);
	break;
    }
    case BMI_OPTIMISTIC_BUFFER_REG:
	break;
    default:
	/* Should return -ENOSYS, but return 0 for caller ease. */
	break;
    }
    return 0;
}

/*
 * This is called with a method_addr initialized by
 * bmip_method_addr_lookup.
 */
static int bmip_initialize(struct bmi_method_addr *listen_addr,
			   int method_id, int init_flags)
{
    int ret = -ENODEV, numint;

    //gossip_enable_stderr();
    //gossip_set_debug_mask(1, GOSSIP_BMI_DEBUG_PORTALS);

    bmip_gen_mutex_lock(&ni_mutex);

    any_pid.nid = PTL_NID_ANY;
    any_pid.pid = PTL_PID_ANY;

    /* check params */
    if (!!listen_addr ^ (init_flags & BMI_INIT_SERVER)) {
	gossip_err("%s: server but no listen address\n", __func__);
	ret = -EIO;
    bmip_gen_mutex_unlock(&ni_mutex);
	goto out;
    }

    bmi_portals_method_id = method_id;

    ret = PtlInit(&numint);
    if (ret) {
	gossip_err("%s: PtlInit failed\n", __func__);
	ret = -EIO;
    bmip_gen_mutex_unlock(&ni_mutex);
	goto out;
    }

/*
 * utcp has shorter names for debug symbols; define catamount to these
 * even though it never prints anything.
 */
#ifndef PTL_DBG_ALL
#define  PTL_DBG_ALL PTL_DEBUG_ALL
#define  PTL_DBG_NI_ALL PTL_DEBUG_NI_ALL
#endif

    PtlNIDebug(PTL_INVALID_HANDLE, PTL_DBG_ALL | PTL_DBG_NI_ALL);
    /* PtlNIDebug(PTL_INVALID_HANDLE, PTL_DBG_ALL | 0x001f0000); */
    /* PtlNIDebug(PTL_INVALID_HANDLE, PTL_DBG_ALL | 0x00000000); */
    /* PtlNIDebug(PTL_INVALID_HANDLE, PTL_DBG_DROP | 0x00000000); */

    /* catamount has different debug symbols, but never prints anything */
    PtlNIDebug(PTL_INVALID_HANDLE, PTL_DEBUG_ALL | PTL_DEBUG_NI_ALL);
    /* PtlNIDebug(PTL_INVALID_HANDLE, PTL_DEBUG_DROP | 0x00000000); */

#if defined(__CRAYXT_SERVICE)
    /*
     * debug
     */
    signal(SIGUSR1, dump_queues);
#endif

    /*
     * Allocate and build MDs for a queue of unexpected messages from
     * all hosts.  Drop lock for coming NI init call.
     */
    //if (init_flags & BMI_INIT_SERVER) {
    {
	bmip_gen_mutex_unlock(&ni_mutex);
	ret = unexpected_init(listen_addr);
	if (ret)
	    PtlFini();
	return ret;
    }

out:
    return ret;
}

/*
 * Shutdown.
 */
static int bmip_finalize(void)
{
    int ret = 0;

    /* do not delete pmas, bmi will call DROP on each for us */

    /*
     * Assuming upper layer has called cancel/test on all the work
     * queue items, else we should probably walk the lists and purge those.
     */

#if 0
    fprintf(stderr, "%s post_recv time = %e, count %i, avg = %e\n", __func__, post_recv_time, post_recv_counter, post_recv_time / post_recv_counter);
    fprintf(stderr, "%s handle_event time = %e, count %i, avg = %e\n", __func__, handle_event_time, handle_event_counter, handle_event_time / handle_event_counter);
    fprintf(stderr, "%s post_send time = %e, count %i, avg = %e\n", __func__, post_send_time, post_send_counter, post_send_time / post_send_counter);
    fprintf(stderr, "%s testcontext time = %e, count %i, avg = %e\n", __func__, testcontext_time, testcontext_counter, testcontext_time / testcontext_counter);
    fprintf(stderr, "%s wfree time = %e, count %i, avg = %e\n", __func__, wfree_time, wfree_counter, wfree_time / walloc_counter);
    fprintf(stderr, "%s walloc time = %e, count %i, avg = %e\n", __func__, walloc_time, walloc_counter, walloc_time / walloc_counter);
#endif

    bmip_gen_mutex_lock(&ni_mutex);

    if (ni == PTL_INVALID_HANDLE)
	goto out;

    nonprepost_fini();
    if (unexpected_buf)
	unexpected_fini();

    dump_queues(0);

    /* destroy connection structures */
    ret = PtlMEUnlink(mark_me);
    if (ret)
	gossip_err("%s: PtlMEUnlink mark: %s\n", __func__, PtlErrorStr(ret));

    ret = PtlMEUnlink(zero_me);
    if (ret)
	gossip_err("%s: PtlMEUnlink zero: %s\n", __func__, PtlErrorStr(ret));

    int i = 0;
    int done = 0;
    bmip_gen_mutex_lock(&eq_mutex);
    while(done == 0)
    { 
        ptl_event_t fevent;  
        ret = PtlEQGet(eq, &fevent);
        if(ret == PTL_OK)
        {
            i++;
        }
        else if(ret == PTL_EQ_EMPTY)
        {
            done = 1;
        }
    }
    ret = PtlEQFree(eq);
    if (ret)
	gossip_err("%s: PtlEQFree: %s\n", __func__, PtlErrorStr(ret));

    bmip_gen_mutex_unlock(&eq_mutex);

    ret = PtlNIFini(ni);
    if (ret)
	gossip_err("%s: PtlNIFini: %s\n", __func__, PtlErrorStr(ret));

    /* do not call this if runtime opened the nic, else oops in sysio */
    if (ni_init_dup == 0)
	PtlFini();

out:
    bmip_gen_mutex_unlock(&ni_mutex);
    return 0;
}

/*
 * All addresses are in the same netmask.
 */
static int bmip_query_addr_range(struct bmi_method_addr *mop __unused,
				 const char *wildcard __unused,
				 int netmask __unused)
{
    return 1;
}

const struct bmi_method_ops bmi_portals_ops =
{
    .method_name = "bmi_portals",
    .flags = 0,
    .initialize = bmip_initialize,
    .finalize = bmip_finalize,
    .set_info = bmip_set_info,
    .get_info = bmip_get_info,
    .memalloc = bmip_memalloc,
    .memfree = bmip_memfree,
    .unexpected_free = bmip_unexpected_free,
    .post_send = bmip_post_send,
    .post_sendunexpected = bmip_post_sendunexpected,
    .post_recv = bmip_post_recv,
    .test = bmip_test,
    .testsome = bmip_testsome,
    .testcontext = bmip_testcontext,
    .testunexpected = bmip_testunexpected,
    .method_addr_lookup = bmip_method_addr_lookup,
    .post_send_list = bmip_post_send_list,
    .post_recv_list = bmip_post_recv_list,
    .post_sendunexpected_list = bmip_post_sendunexpected_list,
    .open_context = bmip_open_context,
    .close_context = bmip_close_context,
    .cancel = bmip_cancel,
    .rev_lookup_unexpected = bmip_rev_lookup,
    .query_addr_range = bmip_query_addr_range,
};

