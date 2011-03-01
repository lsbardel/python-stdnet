    
    
cdef extern from "hiredis_ext.h":

    ctypedef struct redisContext:
        pass
        
    ctypedef struct redisReplyObjectFunctions:
        pass
        
    redisContext* redisConnect(char*, int)
    
    void redisFree(redisContext*)
    
    int isconnected(redisContext*)
    
    void redisAppendCommandArgv(redisContext*, int, char**, int*)
    