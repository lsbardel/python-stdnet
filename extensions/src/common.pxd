    
cdef extern from "parser.h":
    
    cdef cppclass RedisParser:
        RedisParser(object, object) except +
        void feed(const char*)
        object get()
        object get_buffer()