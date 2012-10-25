    
    
cdef extern from "reader.h":
    
    ctypedef struct pythonReader:
        pass
            
    pythonReader* pythonReaderCreate(object pErrCls, object eErrCls)
    
    void pythonReaderFree(pythonReader *r)
    
    void pythonReader_feed(pythonReader *r, object args)
    
    object pythonReader_gets(pythonReader *r)
    
    