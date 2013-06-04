from redis.connection import PythonParser as _p, InvalidResponse
EXCEPTION_CLASSES = _p.EXCEPTION_CLASSES

from .extensions import *
from .client import *
from .info import *
from . import parser
 

try:
    from .async import AsyncConnectionPool
except ImportError:
    AsyncConnectionPool = None
    
def ResponseError(response):
    "Parse an error response"
    error_code = response.split(' ')[0]
    if error_code not in EXCEPTION_CLASSES:
        error_code = 'ERR'
    response = response[len(error_code) + 1:]
    return EXCEPTION_CLASSES[error_code](response)

PythonRedisParser = lambda : parser.RedisParser(InvalidResponse, ResponseError)
RedisParser = PythonRedisParser
    
    
def redis_client(address, connection_pool=None, timeout=None, reader=None,
                 **kwargs):
    '''Get a new redis client'''
    if not connection_pool:
        if timeout == 0:
            if not AsyncConnectionPool:
                raise ImportError('Asynchronous connection requires async '
                                  'bindings installed.')
            connection_pool = AsyncConnectionPool
        else:
            connection_pool = ConnectionPool
        kwargs['parser'] = PythonRedisParser if reader == 'py' else RedisParser
        connection_pool = connection_pool(address, **kwargs)
    return Redis(connection_pool=connection_pool)