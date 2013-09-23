try:
    from . import async
except ImportError:
    async = None

from .extensions import RedisScript, read_lua_file, redis, get_script
from .client import Redis
from .info import RedisDb, RedisKey, RedisDataFormatter

RedisError = redis.RedisError

__all__ = ['redis_client', 'RedisScript', 'read_lua_file', 'RedisError',
           'RedisDb', 'RedisKey', 'RedisDataFormatter', 'get_script']


def redis_client(address=None, connection_pool=None, timeout=None, reader=None,
                 **kwargs):
    '''Get a new redis client'''
    if not connection_pool:
        if timeout == 0:
            if not async:
                raise ImportError('Asynchronous connection requires async '
                                  'bindings installed.')
            return async.pool.redis(address, **kwargs)
        else:
            kwargs['socket_timeout'] = timeout
            return Redis(address[0], address[1], **kwargs)
    else:
        return Redis(connection_pool=connection_pool)
    
