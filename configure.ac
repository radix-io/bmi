dnl **************************************************************
dnl PVFS2 AUTOCONF SCRIPT
dnl
dnl Process this file with autoconf to produce a configure script.
dnl You may need to use autoheader as well if changing any DEFINEs

dnl sanity checks, output header, location of scripts used here
AC_INIT()
AC_PREREQ(2.59)
AC_CONFIG_AUX_DIR(maint/config)

dnl
dnl for test builds, insert the build date and time information into
dnl into the version; for official releases, clear the PVFS2_VERSION_PRE line
dnl
dnl PVFS2 versioning information.  
dnl An @ in the date string can confuse e.g. scp and 'make kmod'
CONFIGURE_TIME=`date -u +"%Y-%m-%d-%H%M%S"`
PVFS2_VERSION_MAJOR=2
PVFS2_VERSION_MINOR=8
PVFS2_VERSION_SUB=1
#PVFS2_VERSION_PRE="pre1-$CONFIGURE_TIME"
PVFS2_VERSION_PRE=""

PVFS2_VERSION=$PVFS2_VERSION_MAJOR.$PVFS2_VERSION_MINOR.$PVFS2_VERSION_SUB$PVFS2_VERSION_PRE
AC_SUBST(PVFS2_VERSION)
AC_DEFINE_UNQUOTED(PVFS2_VERSION_MAJOR, $PVFS2_VERSION_MAJOR, major version number)
AC_DEFINE_UNQUOTED(PVFS2_VERSION_MINOR, $PVFS2_VERSION_MINOR, minor version number)
AC_DEFINE_UNQUOTED(PVFS2_VERSION_SUB, $PVFS2_VERSION_SUB, sub version number)
AC_SUBST(PVFS2_VERSION_MAJOR)
AC_SUBST(PVFS2_VERSION_MINOR)
AC_SUBST(PVFS2_VERSION_SUB)

dnl Checks for host type
dnl Ensures we can compile on this OS
AC_CANONICAL_HOST

USR_CFLAGS=$CFLAGS
if test "x$USR_CFLAGS" = "x"; then
	USR_CFLAGS_SET=no
fi

AC_CONFIG_HEADER(pvfs2-config.h)

AC_CHECK_HEADERS(stdarg.h)

AC_CHECK_SIZEOF([long int])

dnl Check for utilities that we need during the build process
AC_PROG_INSTALL
AC_PROG_CC

AC_MSG_CHECKING([for required gcc])
if test "x$GCC" = "x"; then
	AC_MSG_ERROR(no)
fi

CFLAGS=$USR_CFLAGS

AC_PROG_CPP

if test $host != $build; then
	AC_CHECK_PROGS(BUILD_CC, gcc cc)
else
	BUILD_CC=$CC
	BUILD_CFLAGS=$BUILD_CFLAGS
	BUILD_LDFLAGS=$BUILD_LDFLAGS
fi

AC_SUBST(BUILD_CC)
AC_SUBST(BUILD_CFLAGS)
AC_SUBST(BUILD_LDFLAGS)

#
# Threading options.
#
# Client - thread-safe or not
#   --disable-thread-safety : chooses null or posix locking mechanism
#   For applications that know they are not multi-threaded, or choose
#   to handle locking themselves, this configure option can be used to
#   slightly decrease library-induced latency by removing the locking.
#
# Client - job threading
#   libpvfs2-threaded.{so,a} with __PVFS2_JOB_THREADED__
#   This option causes the client library to spawn an extra thread
#   to handle network communications.  In this way, it is possible
#   that the library can handle new and completed requests from
#   the application while also interacting with the network.
#
#   These separate libraries can not be built using the existing
#   configure mechanism.  They are only built if required by the
#   kernel module helper.
#

dnl a mechanism to turn off threads in the client library
AC_MSG_CHECKING([for client library thread safety support])
AC_ARG_ENABLE(thread-safety,
[  --disable-thread-safety Disables thread safety in the client library],
[if test "x$enableval" = "xno" ; then
    LIBCFLAGS="$LIBCFLAGS -D__GEN_NULL_LOCKING__"
    THREAD_LIB=""
    AC_MSG_RESULT(no)
fi],
[   LIBCFLAGS="$LIBCFLAGS -D__GEN_POSIX_LOCKING__"
    THREAD_LIB="-lpthread"
    AC_MSG_RESULT(yes) ])

AC_SUBST(LIBCFLAGS)
AC_SUBST(THREAD_LIB)

dnl See if the --enable-trusted-connections  option was given to configure
AC_ARG_ENABLE(trusted-connections,
[  --enable-trusted-connections
                          **EXPERIMENTAL** Enable connects only 
                          from trusted hosts/ports ],
[if test "x$enableval" = "xyes"; then
TRUSTED_CONNECTIONS="-DUSE_TRUSTED"
fi],
)
AC_SUBST(TRUSTED_CONNECTIONS)


dnl shared or static client library (see --enable-shared below)
AC_ARG_ENABLE(static,
[  --disable-static        Do not build static client library],
[build_static=$enableval], [build_static=yes])
AC_SUBST(build_static)

dnl NOTE: removing the linux-gnu host check for now; we may want to put it back
dnl before releaseing any code, though...  -Phil
dnl 
dnl AC_MSG_CHECKING(whether we know how to compile for this OS)
dnl if test x$host_os != xlinux-gnu; then AC_MSG_ERROR($host_os not supported.)
dnl else AC_MSG_RESULT(yes)
dnl fi

dnl Check byte ordering
AC_C_BIGENDIAN

dnl Check size of pointer type; needed by id-generator
AC_CHECK_SIZEOF(void *)

dnl Options for debugging
TAU_INCS=
BUILD_TAU=

dnl use the tau trace library
AC_ARG_WITH(tau,
[  --with-tau=path         Use TAU trace library installed in "path"],
    if test "x$withval" = "xyes" ; then
           AC_MSG_ERROR(--with-tau must be given a pathname)
    else
       TAU_INCS="-I$withval/include"
       CFLAGS="$CFLAGS $TAU_INCS -D__PVFS2_ENABLE_EVENT__"
       LDFLAGS="$LDFLAGS -L$withval/$(uname -m)/lib"
       tau_mkfile=$withval/$(uname -m)/lib/Makefile.*-profile-trace
       if test ! -f $tau_mkfile; then
           AC_MSG_ERROR(TAU not compiled with profiling and tracing support)
       fi
       tau_config=$(echo $tau_mkfile | sed -e "s|.*Makefile.tau-||")
       LIBS="$LIBS -lTAU_tf -lTAU_traceinput-$tau_config -ltau-$tau_config -lpthread -lstdc++"
       AC_DEFINE(HAVE_TAU, 1, [Define if TAU library is used])
       BUILD_TAU=1
    fi
)

AC_SUBST(TAU_INCS)
AC_SUBST(BUILD_TAU)

BUILD_ABSOLUTE_TOP=`pwd`
SRC_RELATIVE_TOP=`echo $0 | sed -e "s|configure$||"`
SRC_ABSOLUTE_TOP=`cd $SRC_RELATIVE_TOP ; pwd`
AC_SUBST(BUILD_ABSOLUTE_TOP)
AC_SUBST(SRC_RELATIVE_TOP)
AC_SUBST(SRC_ABSOLUTE_TOP)

dnl Optimized code, but never override CFLAGS from command line or environment.
AC_ARG_ENABLE(fast,
[  --enable-fast           Disable optional debugging, enable optimizations.],
[
if test "x$USR_CFLAGS_SET" = "xno"; then
    CFLAGS="$CFLAGS -DNDEBUG -O3 -DGOSSIP_DISABLE_DEBUG"
fi
])

# default CFLAGS is -g -O2, unless user set CFLAGS or asked for --enable-fast
if test "x$USR_CFLAGS_SET" = "xno" && test "x$enable_fast" != "xyes"; then
    CFLAGS="$CFLAGS -g -O2"
fi

STRICT_CFLAGS=
AC_ARG_ENABLE(strict,
[  --enable-strict         Turn on strict compiler warnings],
    STRICT_CFLAGS=1)
AC_SUBST(STRICT_CFLAGS)

dnl Check if user actually wants to see all our build output (compiles, links, etc.).
AC_ARG_ENABLE(verbose-build,
[  --enable-verbose-build  Enables full output during build process],
QUIET_COMPILE=0,
QUIET_COMPILE=1)

AC_SUBST(QUIET_COMPILE)

dnl
dnl a function to check for epoll capabilities
dnl
test_for_epoll()
{
  AC_CHECK_HEADERS(sys/epoll.h)
  AC_MSG_CHECKING(for epoll functions)
  AC_TRY_LINK([#include <sys/epoll.h>
               #include <unistd.h>],
  [
	int fd;
	fd = epoll_create(1);
	close(fd);
  ],
     AC_MSG_RESULT(yes)
     BUILD_EPOLL=1
     AC_SUBST(BUILD_EPOLL),
     AC_MSG_RESULT(no)
  )
}

dnl optionally enable use of epoll() on Linux instead of poll()
dnl optionally disable use of epoll() on Linux instead of poll()
AC_ARG_ENABLE(epoll,
[  --enable-epoll          Enable use of epoll for TCP/IP polling 
                          (epoll is enabled by default, this option
			  ensures no checking is done for x-compiling)
  --disable-epoll         Disable epoll, use poll instead],
[
   if test "x$enableval" = "xyes"; then
        BUILD_EPOLL=1
        AC_SUBST(BUILD_EPOLL)
   fi
],
[
  test_for_epoll
])

dnl enables a hack to print back traces out of segfault signal handler
AC_ARG_ENABLE(segv-backtrace,
[  --disable-segv-backtrace
                          Disables back traces in segfault signal handler],,
    AC_MSG_CHECKING(if segv backtrace capable)
    AC_TRY_COMPILE([
#include <execinfo.h>
#define __USE_GNU
#include <ucontext.h>
#if !defined(REG_EIP) && !defined(REG_RIP)
		   choke me
#endif
		   ], [],
		   AC_MSG_RESULT(yes)
    		   PVFS2_SEGV_BACKTRACE=1,
		   AC_MSG_RESULT(no)
    )
)
AC_SUBST(PVFS2_SEGV_BACKTRACE)

dnl shared or static client library
AC_ARG_ENABLE(shared,
[  --enable-shared         Build shared client library],
[build_shared=$enableval], [build_shared=no])
AC_SUBST(build_shared)

dnl Bad things happen in the makefile if you happen to --disable-static
dnl but do not --enable-shared.
if test "x$build_shared" = "xno" -a "x$build_static" = "xno" ; then
    AC_MSG_ERROR([Must do --enable-shared or --enable-static or both.])
fi

dnl See if CC is a GNU compiler.  This may require a real test in future
dnl versions of autoconf.  In 2.13 it is a side-effect of AC_PROG_CC.  First
dnl check if it is an Intel compiler; those lie and claim to be gcc but are
dnl not argument compatible
INTELC=
GNUC=
AC_MSG_CHECKING(whether cc is an Intel compiler)
AC_TRY_COMPILE([
#ifndef __ICC
       choke me
#endif
], [],
    AC_MSG_RESULT(yes)
    INTELC=1
    ,
    AC_MSG_RESULT(no)
)
if test "x$INTELC" = "x" ; then
    if test "x$GCC" = "xyes" ; then
       GNUC=1
    fi
fi
AC_SUBST(INTELC)
AC_SUBST(GNUC)

case "$host_os" in

    *darwin*)
	AC_DEFINE([TARGET_OS_DARWIN], 1, Define if on darwin)
    	TARGET_OS_DARWIN=1
	AC_SUBST(TARGET_OS_DARWIN)
    ;;
    *linux*)
	AC_DEFINE([TARGET_OS_LINUX], 1, Define if on linux)
    	TARGET_OS_LINUX=1
	AC_SUBST(TARGET_OS_LINUX)
    ;;
esac

AC_CHECK_HEADERS(netdb.h)
AC_CHECK_HEADERS(arpa/inet.h)
AC_CHECK_HEADERS(sys/socket.h)
dnl check for gethostbyname function
AC_MSG_CHECKING(for gethostbyname)
oldcflags="$CFLAGS"
CFLAGS="$USR_CFLAGS"
AC_TRY_COMPILE([
#include <netdb.h>
], [
gethostbyname("localhost");
],
AC_MSG_RESULT(yes)
AC_DEFINE(HAVE_GETHOSTBYNAME, 1, Define if gethostbyname function exists),
AC_MSG_RESULT(no)
WARN_ABOUT_HOSTNAMES="yes")

AC_MSG_CHECKING(for gethostbyaddr)
AC_TRY_COMPILE([
#include <netdb.h>
], [
struct sockaddr_in peer;
gethostbyaddr((void *)&peer.sin_addr.s_addr, sizeof(struct in_addr), AF_INET);
],
AC_MSG_RESULT(yes)
AC_DEFINE(HAVE_GETHOSTBYADDR, 1, Define if gethostbyaddr function exists),
AC_MSG_RESULT(no)  )

CFLAGS="$oldcflags"

dnl for regular functions, add another AC_CHECK_FUNCS line
AC_CHECK_FUNCS(strnlen)
AC_CHECK_FUNCS(strtoull)
AC_CHECK_FUNCS(strstr)

dnl if we use headers that might not be on every platform, add them here
AC_CHECK_HEADERS(malloc.h)

dnl this is a special case, used to key off options controlling gossip
dnl   backtrace capability
AC_CHECK_HEADERS(execinfo.h,
	LDFLAGS="$LDFLAGS -rdynamic"
	GOSSIP_ENABLE_BACKTRACE=1
)
AC_SUBST(GOSSIP_ENABLE_BACKTRACE)

dnl Which form of strerror?
AC_MSG_CHECKING([for GNU-style strerror_r])
AC_TRY_COMPILE(
    [
	#include <string.h>
	extern char *strerror_r();
    ], [],
    AC_MSG_RESULT(yes)
    AC_DEFINE(HAVE_GNU_STRERROR_R, 1, Define if strerror_r is GNU-specific)
    ,
    AC_MSG_RESULT(no)
)

dnl create any missing subdirectories that wouldn't be created
dnl by AC_OUTPUT below (which won't create intermediate dirs).
for d in src/apps src/io src/common src/client src/kernel \
       doc src/apps/kernel test; do
       install -d $d;
done

dnl output final version of top level makefile and subdirectory
dnl    makefile includes
AC_OUTPUT(include/pvfs2.h
Makefile
module.mk
src/common/misc/module.mk
src/common/quickhash/module.mk
src/common/quicklist/module.mk
src/common/id-generator/module.mk
src/common/gossip/module.mk
src/common/gen-locks/module.mk
src/io/bmi/module.mk
src/io/bmi/bmi_tcp/module.mk
src/proto/module.mk
)


# print a summary of the configuration information
AC_MSG_RESULT()
AC_MSG_RESULT([***** Displaying BMI configuration information *****])
AC_MSG_RESULT([----------------------------------------------------])
PKGSTR="BMI"

# print BMI configs
if test "x$THREAD_LIB" = "x"; then
   AC_MSG_RESULT([$PKGSTR configured for a thread-safe client library :  no])
else
   AC_MSG_RESULT([$PKGSTR configured for a thread-safe client library : yes])
fi

if test "x$BUILD_EPOLL" = "x1" ; then
   AC_MSG_RESULT([$PKGSTR configured to use epoll                     : yes])
else
   AC_MSG_RESULT([$PKGSTR configured to use epoll                     :  no])
fi

if test "x$TRUSTED_CONNECTIONS" = "x" ; then
	AC_MSG_RESULT([$PKGSTR configured for using trusted connections    :  no])
else
	AC_MSG_RESULT([$PKGSTR configured for using trusted connections    : yes])
fi

if test "x$WARN_ABOUT_HOSTNAMES" = "xyes" ; then
   AC_MSG_RESULT(WARNING: gethostbyname is not supported on this machine: ALL ADDRESSES MUST BE IN DOT NOTATION.)
fi


AC_MSG_RESULT()
AC_MSG_RESULT([$PKGSTR version string: $PVFS2_VERSION])

AC_MSG_RESULT()
