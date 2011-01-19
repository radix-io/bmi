#ifndef PORTALS_H
#define PORTALS_H

#include "quicklist/quicklist.h"
#include "gen-locks/gen-locks.h"
#include "bmi.h"
#include "bmi-method-support.h"

/* portals.c */

extern int portals_method_id;
extern const struct bmi_method_ops bmi_portals_ops;

#endif
