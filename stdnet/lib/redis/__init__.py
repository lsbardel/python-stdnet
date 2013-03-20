from stdnet.lib import hiredis, fallback

from .extensions import *
from .client import *
from .redisinfo import *

try:
    from .async import AsyncConnectionPool, PubSub
except ImportError:
    AsyncConnectionPool = None
    PubSub = None

PyRedisReader = lambda : fallback.RedisReader(InvalidResponse, ResponseError)

if hiredis:  #pragma    nocover
    RedisReader = lambda : hiredis.Reader(InvalidResponse, ResponseError)
else:   #pragma    nocover
    RedisReader = PyRedisReader
    
def redis_client(address, connection_pool=None, timeout=None, reader=None,
                  **kwargs):
    if not connection_pool:
        if timeout == 0:
            if not AsyncConnectionPool:
                raise ImportError('Asynchronous redis connection requires pulsar')
            connection_pool = AsyncConnectionPool
        else:
            connection_pool = ConnectionPool
        kwargs['reader'] = PyRedisReader if reader == 'py' else RedisReader
        connection_pool = connection_pool(address, **kwargs)
    return Redis(connection_pool=connection_pool)
