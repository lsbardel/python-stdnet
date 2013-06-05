cdef extern from "parser.h":
    
    cdef cppclass RedisParser:
        RedisParser(object, object) except +
        void feed(const char*)
        object gets()


cdef class CRedisParser:
    '''Cython wrapper for Hiredis protocol parser.'''
    
    cdef RedisParser *_parser

    def __cinit__(self, object perr, object rerr):
        self._parser = new RedisParser(perr, rerr)
        
    def __dealloc__(self):
        if self._parser is not NULL:
            del self._parser
        
    def feed(self, object stream):
        self._parser.feed(stream)
        
    def gets(self):
        return self._parser.gets()
        