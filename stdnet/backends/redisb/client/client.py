'''The :mod:`stdnet.backends.redisb.client` implements several extensions
to the standard redis client in redis-py_


Client
~~~~~~~~~~~~~~

.. autoclass:: Redis
   :members:
   :member-order: bysource

Prefixed Client
~~~~~~~~~~~~~~~~~~

.. autoclass:: PrefixedRedis
   :members:
   :member-order: bysource

RedisScript
~~~~~~~~~~~~~~~

.. autoclass:: RedisScript
   :members:
   :member-order: bysource

'''
import os
import io
import socket
from copy import copy

from .extensions import RedisExtensionsMixin, redis, BasePipeline
from .prefixed import PrefixedRedisMixin


class Redis(RedisExtensionsMixin, redis.StrictRedis):

    @property
    def encoding(self):
        return self.connection_pool.connection_kwargs.get('encoding', 'utf-8')

    def address(self):
        kw = self.connection_pool.connection_kwargs
        return (kw['host'], kw['port'])

    def prefixed(self, prefix):
        '''Return a new :class:`PrefixedRedis` client.
        '''
        return PrefixedRedis(self, prefix)

    def pipeline(self, transaction=True, shard_hint=None):
        return Pipeline(
            self,
            transaction,
            shard_hint)


class PrefixedRedis(PrefixedRedisMixin, Redis):
    pass


class Pipeline(BasePipeline, Redis):

    def __init__(self, client, transaction, shard_hint):
        self.client = client
        self.response_callbacks = client.response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.connection = None
        self.reset()

    @property
    def connection_pool(self):
        return self.client.connection_pool

    @property
    def is_pipeline(self):
        return True
