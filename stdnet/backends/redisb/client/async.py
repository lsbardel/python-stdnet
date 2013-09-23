'''The :mod:`stdnet.backends.redisb.async` module implements an asynchronous
connector for redis-py_. It uses pulsar_ asynchronous framework.
To use this connector,
add ``timeout=0`` to redis :ref:`connection string <connection-string>`::

    'redis://127.0.0.1:6378?password=bla&timeout=0'

Usage::

    from stdnet import getdb
    
    db = getdb('redis://127.0.0.1:6378?password=bla&timeout=0')
    
    
.. _redis_pubsub:

Asynchronous Publish/Subscribe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PubSub
   :members:
   :member-order: bysource
    
'''
from pulsar.apps import redis

from .extensions import (RedisExtensionsMixin, get_script, RedisError,
                         all_loaded_scripts)
from .prefixed import PrefixedRedisMixin

from stdnet.utils.async import async_binding, async

if not async_binding:   #pragma    nocover
    raise ImportError


class Redis(redis.Redis, RedisExtensionsMixin):
    
    def address(self):
        return self.connection_info[0]

    def prefixed(self, prefix):
        '''Return a new :class:`PrefixedRedis` client.
        '''
        return PrefixedRedis(self, prefix)

    @async()
    def execute_script(self, name, keys, *args, **options):
        script = get_script(name)
        if not script:
            raise redis.RedisError('No such script "%s"' % name)
        address = self.address()
        if address not in all_loaded_scripts:
            all_loaded_scripts[address] = set()
        loaded = all_loaded_scripts[address]
        toload = script.required_scripts.difference(loaded)
        for name in toload:
            s = get_script(name)
            yield self.script_load(s.script)
        loaded.update(toload)
        yield script(self, keys, args, options)


class PrefixedRedis(PrefixedRedisMixin, Redis):
    pass


class RedisClient(redis.RedisClient):
    
    def redis(self, address, db=0, password=None, timeout=None, **kw):
        '''Return a :class:`Redis` client.
        
        :param address: the address of the server.
        :param address: server database number.
        :param password: optional server password.
        :param timeout: optional timeout for idle connections.
        '''
        timeout = int(timeout or self.timeout)
        info = redis.connection_info(address, db, password, timeout)
        return Redis(self, info, **kw)


pool = RedisClient()
