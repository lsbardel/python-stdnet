cimport common

cdef class RedisParser:
    '''Cython wrapper for Hiredis protocol parser.'''
    
    cdef common.RedisParser *_parser

    def __cinit__(self, object perr, object rerr):
        self._parser = new common.RedisParser(perr, rerr)
        
    def __dealloc__(self):
        if self._parser is not NULL:
            del self._parser
        
    def feed(self, object stream):
        self._parser.feed(stream)
        
    def get(self):
        return self._parser.get()
        