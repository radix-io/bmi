#ifndef PORTALS_HELPERS_H
#define PORTALS_HELPERS_H

#include <portals/portals3.h>
#include <sys/utsname.h>
#include <stdarg.h>
#include <stdio.h>

#include <time.h>

void bmip_fprintf(const char * fmt, const char * file, const char * func, const int line);
void bmip_fprintf_err(const char * fmt, int err, const char * errstr, const char * file, const char * func, const int line);

#ifndef HAVE_PTLERRORSTR
const char *PtlErrorStr(unsigned int ptl_errno);
#endif

#ifndef HAVE_PTLEVENTKINDSTR
const char *PtlEventKindStr(ptl_event_kind_t ev_kind);
#endif

int bmip_get_time(struct timespec * timeval);

double bmip_elapsed_time(struct timespec * t1, struct timespec * t2);

int binbuffer_to_text(void * buffer, char * buf, int size);

#endif /* PORTALS_HELPERS_H */
