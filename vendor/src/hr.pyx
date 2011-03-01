cimport common

from numpy cimport *

cdef extern from "numpy/arrayobject.h":
    void import_array()


cdef class redisConnection:
    '''Manage connection with redis server using hiredis'''
    
    cdef common.redisContext *_c_connection
    
    def __cinit__(self, host = 'localhost', port = 6379):
        self._c_connection = common.redisConnect(host, port)
        if self._c_connection is NULL:
            raise MemoryError()
        
    def __dealloc__(self):
        if self._c_connection is not NULL:
            common.redisFree(self._c_connection)
            
    def isconnected(self):
        return common.isconnected(self._c_connection) == 1
    
    def send(self, command, ndarray args):
        '''Send a command to the redis server'''
        if self.isconnected():
            shape = args.shape
            #redisAppendCommandArgv(self._c_connection,)
            
            
            


 