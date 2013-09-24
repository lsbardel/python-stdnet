try:
    from . import async
except ImportError:
    async = None

from .extensions import (RedisScript, read_lua_file, redis, get_script,
                         RedisDb, RedisKey, RedisDataFormatter)
from .client import Redis

RedisError = redis.RedisError

__all__ = ['redis_client', 'RedisScript', 'read_lua_file', 'RedisError',
           'RedisDb', 'RedisKey', 'RedisDataFormatter', 'get_script']


def redis_client(address=None, connection_pool=None, timeout=None,
                 parser=None, **kwargs):
    '''Get a new redis client.

    :param address: a ``host``, ``port`` tuple.
    :param connection_pool: optional connection pool.
    :param timeout: socket timeout.
    :param timeout: socket timeout.
    '''
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
