try:
    import hiredis
except:
    hiredis = None
hiredis = None

from .extensions import *
from .client import *
from .info import *
from . import parser 

try:
    from .async import AsyncConnectionPool
except ImportError:
    AsyncConnectionPool = None

PythonRedisParser = lambda : parser.RedisParser(InvalidResponse, ResponseError)

if hiredis:  #pragma    nocover
    RedisParser = lambda : hiredis.Reader(InvalidResponse, ResponseError)
else:   #pragma    nocover
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