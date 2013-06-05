    
    
cdef extern from "reader.h":
    
    cdef cppclass RedisParser:
        RedisParser((object, object) except +
        void feed(object data)
        object gets()
    
    