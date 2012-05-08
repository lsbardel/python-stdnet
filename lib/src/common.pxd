    
    
cdef extern from "reader.h":
    
    ctypedef struct pythonReader:
        pass
            
    pythonReader* pythonReaderCreate(object pErrCls, object eErrCls)
    
    void pythonReaderFree(pythonReader *r)
    
    void pythonReaderFeed(pythonReader *r, char *buf, size_t len)
    
    object redisRead(pythonReader *r)
    
    