#ifndef __HIREDIS_EXT_H
#define __HIREDIS_EXT_H

#include "hiredis.h"


inline int isconnected(redisContext* r) {
    if(r && !r->err)
        return 1;
    else
        return 0;
}


#endif  //  __HIREDIS_EXT_H

