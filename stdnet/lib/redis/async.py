'''The :mod:`stdnet.lib.redis.async` implements an asynchronous connector
for redis-py. It uses pulsar_ asynchronous framework. To use this connector,
add ``timeout=0`` to redis :ref:`connection string <connection-string>`::

    'redis://127.0.0.1:6378?password=bla&timeout=0'

Usage::

    from stdnet import getdb
    
    db = getdb('redis://127.0.0.1:6378?password=bla&timeout=0')
    
    
.. _pulsar: https://pypi.python.org/pypi/pulsar
'''
from collections import deque
from itertools import chain
from functools import partial

import redis

import pulsar
from pulsar import is_async, multi_async, get_actor, Deferred
from pulsar.utils.pep import ispy3k, map

from .extensions import get_script, RedisManager


class AsyncRedisRequest(object):
    '''Asynchronous Request for redis.'''
    def __init__(self, client, connection, timeout, encoding,
                  encoding_errors, command_name, args, reader,
                  raise_on_error=True, on_finished=None,
                  **options):
        self.client = client
        self.connection = connection
        self.timeout = timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.command_name = command_name.upper()
        self.reader = reader
        self.raise_on_error = raise_on_error
        self.on_finished = on_finished
        self.response = []
        self.last_response = False
        self.args = args
        pool = client.connection_pool
        if client.is_pipeline:
            self.command = pool.pack_pipeline(args)
        else:
            self.command = pool.pack_command(self.command_name, *args)
            args =(((command_name,), options),)
        self.args_options = deque(args)
        
    @property
    def key(self):
        return (self.connection, self.timeout)
    
    @property
    def address(self):
        return self.connection.address
    
    @property
    def is_pipeline(self):
        return not bool(self.command_name)
    
    def __repr__(self):
        if self.is_pipeline:
            return 'PIPELINE%s' % self.args
        else:
            return '%s%s' % (self.command_name, self.args)
    __str__ = __repr__
    
    def read_response(self):
        # For compatibility with redis-py
        return self.last_response
    
    def feed(self, data):
        self.reader.feed(data)
        while self.args_options:
            self.last_response = response = self.reader.gets()
            if response is False:
                break
            args, options = self.args_options.popleft()
            if not isinstance(response, Exception):
                response = self.client.parse_response(self, args[0], **options)
            self.response.append(response)
        if self.last_response is not False:
            return self.client.on_response(self.response, self.raise_on_error)
    
    
class RedisProtocol(pulsar.ProtocolConsumer):
    '''An asynchronous pulsar protocol for redis.'''
    reader = None
    def start_request(self):
        self.reader = self.current_request.reader
        self.transport.write(self.current_request.command)
    
    def data_received(self, data):
        finished = False
        req = self.current_request
        if req:
            response = req.feed(data)
            if response is not None and req.on_finished:
                self._current_request = None
                req.on_finished.callback((self, response))
            else:
                finished = True
        else:
            self.reader.feed(data)
            response = self.reader.gets()
        if response is not None:
            self.fire_event('on_message', response)
            if finished:
                self.finished(response)
                
    def on_response(self, response):
        pass
    
class AsyncConnectionPool(pulsar.Client, RedisManager):
    '''A :class:`pulsar.Client` for managing a connection pool with redis
data-structure server.'''
    connection_pools = {}
    consumer_factory = RedisProtocol
    
    def __init__(self, address, db=0, password=None, encoding=None, reader=None,
                 encoding_errors='strict', **kwargs):
        super(AsyncConnectionPool, self).__init__(**kwargs)
        self.encoding = encoding or 'utf-8'
        self.encoding_errors = encoding_errors or 'strict'
        self.password = password
        self._setup(address, db, reader)
    
    def request(self, client, command_name, *args, **options):
        consumer = options.pop('consumer', None)
        request = self._new_request(client, command_name, args, **options)
        return self.response(request, consumer=consumer)
    
    def request_pipeline(self, pipeline, raise_on_error=True):
        commands = pipeline.command_stack
        if not commands:
            return ()
        if pipeline.is_transaction:
            commands = list(chain([(('MULTI', ), {})], commands,
                                  [(('EXEC', ), {})]))
        request = self._new_request(pipeline, '', commands,
                                    raise_on_error=raise_on_error)
        return self.response(request)
        
    def response(self, request, consumer=None):
        first_request = request
        if not consumer:
            connection = self.get_connection(request)
            consumer = self.consumer_factory(connection)
            # If this is a new connection we need to select database and login
            if not connection.processed:
                reqs = []
                client = request.client
                if client.is_pipeline:
                    client = client.client
                c = self.connection
                if self.password:
                    reqs.append(self._new_request(client,
                            'auth', (self.password,), on_finished=Deferred()))
                if c.db:
                    reqs.append(self._new_request(client,
                            'select', (c.db,), on_finished=Deferred()))
                reqs.append(request)
                for req, next in zip(reqs, reqs[1:]):
                    req.on_finished.add_callback(
                                    partial(self._next, consumer, next))
                first_request = reqs[0]
        consumer.new_request(first_request)
        return request.on_finished or consumer.on_finished
            
    def _new_request(self, client, command_name, args, **options):
        return AsyncRedisRequest(client, self.connection, self.timeout,
                                 self.encoding, self.encoding_errors,
                                 command_name, args, self.redis_reader(),
                                 **options)
    
    def _next(self, consumer, next_request, result):
        consumer.new_request(next_request)
        
    def execute_script(self, client, to_load, callback):
        # Override execute_script so that we execute after scripts have loaded
        if to_load:
            results = []
            for name in to_load:
                s = get_script(name)
                results.append(client.script_load(s.script))
            return multi_async(results).add_callback(callback)
        else:
            return callback()
        