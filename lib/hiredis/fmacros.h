#ifndef __HIREDIS_FMACRO_H
#define __HIREDIS_FMACRO_H

#if !defined(_BSD_SOURCE)
#define _BSD_SOURCE
#endif

#if defined(__sun__)
#define _POSIX_C_SOURCE 200112L
#elif defined(__linux__)
#define _XOPEN_SOURCE 600
#else
#define _XOPEN_SOURCE
#endif

#if defined(WIN32)
inline void strerror_r(int errnum, char *buf, unsigned int buflen) {
	return;
}
#endif

#endif
