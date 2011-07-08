"Core exceptions raised by the Redis client"

EMPTY = b''
CRLF = b'\r\n'
STAR = b'*'
DOLLAR = b'$'
OK = b'OK'
ERR = b'ERR '
LOADING = b'LOADING '

class RedisError(Exception):
    pass
    
class AuthenticationError(RedisError):
    pass
    
class ConnectionError(RedisError):
    pass
    
class ResponseError(RedisError):
    pass
    
class InvalidResponse(RedisError):
    pass
    
class InvalidData(RedisError):
    pass
    