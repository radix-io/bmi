#include "portals_comm.h"

#include <stdio.h>

const char * bmip_ptl_ev_type(ptl_event_t * ev)
{
	switch(ev->type)
	{
                case PTL_EVENT_SEND_START:
			return "PTL_EVENT_SEND_START";
                case PTL_EVENT_SEND_END:
			return "PTL_EVENT_SEND_END";
                case PTL_EVENT_PUT_START:
			return "PTL_EVENT_PUT_START";
                case PTL_EVENT_PUT_END:
			return "PTL_EVENT_PUT_END";
                case PTL_EVENT_ACK:
			return "PTL_EVENT_ACK";
                case PTL_EVENT_GET_START:
			return "PTL_EVENT_GET_START";
                case PTL_EVENT_GET_END:
			return "PTL_EVENT_GET_END";
                case PTL_EVENT_REPLY_START:
			return "PTL_EVENT_REPLY_START";
                case PTL_EVENT_REPLY_END:
			return "PTL_EVENT_REPLY_END";
                case PTL_EVENT_UNLINK:
			return "PTL_EVENT_UNLINK";
                default:
                        return "UNKNOWN";
	};
out:
	return NULL;
}

int bmip_unex_handler(ptl_event_t * ev)
{
	int ret = ev->type;

	//fprintf(stderr, "%s: event type %s, nid %i, pid %i, match_bits %llx, rlength %i, mlength %i, offset %i hdr data = %llu\n", __func__, bmip_ptl_ev_type(ev), ev->initiator.nid, ev->initiator.pid, ev->match_bits, ev->rlength, ev->mlength, ev->offset, ev->hdr_data);
	switch(ev->type)
	{
		case PTL_EVENT_SEND_START:
			break;	
		case PTL_EVENT_SEND_END:
			break;	
		case PTL_EVENT_PUT_START:
			break;	
		case PTL_EVENT_PUT_END:
			break;	
		case PTL_EVENT_ACK:
			break;	
		case PTL_EVENT_GET_START:
			break;	
		case PTL_EVENT_GET_END:
			break;	
		case PTL_EVENT_REPLY_START:
			break;	
		case PTL_EVENT_REPLY_END:
			break;	
		case PTL_EVENT_UNLINK:
			break;	
		default:
			ret = -1;
			break;	
	};
out:
	return ret;
}

int bmip_wait_event(int timeout, ptl_handle_eq_t * eq, ptl_event_t * ev)
{	
	int ret = -1;
	int i = 0;
	const int numhandles = 1;
	ptl_event_t sev;
	ptl_event_t * lev;

	/* detect if we want a copy of the event data or not */
	if(ev == NULL)
	{
		lev = &sev;
	}
	else
	{
		lev = ev;
	}

	/* wait for an unexpected message */
#ifndef BMIP_USE_TIMEOUT
	ret = bmip_ptl_eq_wait(*eq, lev);
#else
	ret = bmip_ptl_eq_poll(eq, numhandles, timeout, lev, &i);
#endif
	if(ret != PTL_EQ_EMPTY)
	{
		if(ret != PTL_OK)
		{
			fprintf(stderr, "eq wait failure\n");
			ret = -1;
			goto out;
		}
		else
		{
			ret = bmip_unex_handler(lev);
			if(ret == -1)
			{
				ret = -1;
				fprintf(stderr, "ev handler failure\n");
				goto out;
			}
		}
	}
	else
	{
		ret = -2;
	}
out:
	return ret;
}

int bmip_msg_send()
{
	int ret = 0;

	ret = bmip_ptl_put();
	if(ret != PTL_OK)
	{
		goto out;
	}
out:
	return ret;
}
