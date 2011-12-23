#ifndef PORTALS_COMM_H
#define PORTALS_COMM_H

#include <portals/portals3.h>
#include <sys/utsname.h>

int bmip_wait_event(int timeout, ptl_handle_eq_t * eq, ptl_event_t * ev);

#endif
