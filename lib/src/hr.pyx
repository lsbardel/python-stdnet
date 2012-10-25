cimport common


cdef class RedisReader:
    '''Cython wrapper for Hiredis protocol parser.'''
    
    cdef common.pythonReader *_c_reader

    def __cinit__(self, perr, rerr):
        self._c_reader = common.pythonReaderCreate(perr, rerr)
        if self._c_reader is NULL:
            raise MemoryError()
        
    def __dealloc__(self):
        if self._c_reader is not NULL:
            common.pythonReaderFree(self._c_reader)
            self._c_reader = NULL
        
    def feed(self, object stream):
        common.pythonReader_feed(self._c_reader, stream)
        
    def gets(self):
        return common.pythonReader_gets(self._c_reader)
        