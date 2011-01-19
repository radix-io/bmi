#include "portals_helpers.h"

void bmip_fprintf(const char * fmt, const char * file, const char * func, const int line)
{
	fprintf(stderr, "(%s : %s : %i) %s\n", file, func, line, fmt);
}

void bmip_fprintf_err(const char * fmt, int err, const char * errstr, const char * file, const char * func, const int line)
{
	fprintf(stderr, "(%s : %s : %i) %s, err = %i, errstr = %s\n", file, func, line, fmt, err, errstr);
}

#ifndef HAVE_PTLERRORSTR
const char *PtlErrorStr(unsigned int ptl_errno)
{
	return ptl_err_str[ptl_errno];
}
#endif

#ifndef HAVE_PTLEVENTKINDSTR
const char *PtlEventKindStr(ptl_event_kind_t ev_kind)
{
	extern const char *ptl_event_str[];

	return ptl_event_str[ev_kind];
}
#endif

int bmip_get_time(struct timespec * timeval)
{
    int ret = 0;
    ret = clock_gettime( CLOCK_REALTIME, timeval );

    if(ret != 0)
        fprintf(stderr, "%s:%i timer error\n", __func__, __LINE__);
    return 0;
}

double bmip_elapsed_time(struct timespec * t1, struct timespec * t2)
{
    return ((double) (t2->tv_sec - t1->tv_sec) +
        1.0e-9 * (double) (t2->tv_nsec - t1->tv_nsec) );
}
