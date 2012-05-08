"Core exceptions raised by the Redis client"


class RedisError(Exception):
    pass

    
class AuthenticationError(RedisError):
    pass

    
class ConnectionError(RedisError):
    def __init__(self, msg, retry = False):
        self.retry = retry
        super(ConnectionError,self).__init__(msg)

    
class ResponseError(RedisError):
    pass


class NoScriptError(RedisError):
    msg = 'NOSCRIPT No matching script. Please use EVAL.'
    def __repr__(self):
        return self.msg
    __str__ = __repr__ 
    
class ScriptError(RedisError):
    pass

    
class InvalidResponse(ResponseError):
    pass

    
class InvalidData(RedisError):
    pass
    