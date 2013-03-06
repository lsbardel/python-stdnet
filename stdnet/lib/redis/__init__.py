from .scripts import *
from .client import *
from .redisinfo import *

def redis_client(address, connection_pool=None, timeout=None, **kwargs):
    if not connection_pool:
        if timeout == 0:
            if not AsyncConnectionPool:
                raise ImportError('Asynchronous redis connection requires pulsar')
            connection_pool = AsyncConnectionPool
        else:
            connection_pool = ConnectionPool
        connection_pool = connection_pool(address, **kwargs)
    return Redis(connection_pool=connection_pool)
        
