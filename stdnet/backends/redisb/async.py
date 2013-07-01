'''The :mod:`stdnet.backends.redisb.async` implements an asynchronous connector
for redis-py_. It uses pulsar_ asynchronous framework. To use this connector,
add ``timeout=0`` to redis :ref:`connection string <connection-string>`::

    'redis://127.0.0.1:6378?password=bla&timeout=0'

Usage::

    from stdnet import getdb
    
    db = getdb('redis://127.0.0.1:6378?password=bla&timeout=0')
    
'''
from collections import deque
from itertools import chain
from functools import partial

import redis
from redis.exceptions import NoScriptError

from stdnet.utils.async import async_binding, multi_async, async

if not async_binding:   #pragma    nocover
    raise ImportError

import pulsar
from pulsar import Deferred, NOT_DONE
from pulsar.utils.pep import ispy3k, map

from .extensions import get_script, RedisManager, CppRedisManager


class AsyncRedisRequest(object):
    '''Asynchronous Request for redis.'''
    pubsub_commands = frozenset(('SUBSCRIBE', 'UNSUBSCRIBE',
                                 'PSUBSCRIBE', 'PUNSUBSCRIBE'))
    def __init__(self, client, connection, timeout, encoding,
                  encoding_errors, command_name, args, parser,
                  raise_on_error=True, on_finished=None,
                  release_connection=True, **options):
        self.client = client
        self.connection = connection
        self.timeout = timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.command_name = command_name.upper()
        self.parser = parser
        self.raise_on_error = raise_on_error
        self.on_finished = on_finished
        self.release_connection = release_connection
        self.response = []
        self.last_response = False
        self.args = args
        pool = client.connection_pool
        if client.is_pipeline:
            self.command = pool.pack_pipeline(args)
        elif self.command_name:
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
        self.parser.feed(data)
        response = self.parser.get()
        while response is not False:
            self.last_response = response
            if self.args_options:
                args, options = self.args_options.popleft()
            else:
                args, options = (self.command_name,), {}
            if not isinstance(response, Exception):
                response = self.client.parse_response(self, args[0], **options)
            self.response.append(response)
            response = self.parser.get()
        if not self.args_options:
            return self.client.on_response(self.response, self.raise_on_error)
        else:
            return NOT_DONE
    
    
class RedisProtocol(pulsar.ProtocolConsumer):
    '''An asynchronous pulsar protocol for redis.'''
    parser = None
    release_connection = True
    
    def data_received(self, data):
        response = self.current_request.feed(data)
        if response is not NOT_DONE:
            on_finished = self.current_request.on_finished
            if on_finished and not on_finished.done():
                on_finished.callback(response)
            elif self.release_connection:
                self.finished(response)
    
    def start_request(self):
        # If this is the first request and the connection is new do
        # the login/database switch
        if self.connection.processed <= 1 and self.request_processed == 1:
            request = self.current_request
            reqs = []
            client = request.client
            if client.is_pipeline:
                client = client.client
            producer = self.producer
            c = producer.connection
            if producer.password:
                reqs.append(producer._new_request(client,
                        'auth', (producer.password,), on_finished=Deferred()))
            if c.db:
                reqs.append(producer._new_request(client,
                        'select', (c.db,), on_finished=Deferred()))
            reqs.append(request)
            for req, next in zip(reqs, reqs[1:]):
                req.on_finished.add_callback(partial(self._next, next))
            self._current_request = reqs[0]
        self.transport.write(self.current_request.command)
        
    def _next(self, request, r):
        return self.new_request(request)

    
class AsyncConnectionPoolBase(pulsar.Client):
    '''A :class:`pulsar.Client` for managing a connection pool with redis
data-structure server.'''
    connection_pools = {}
    consumer_factory = RedisProtocol
    
    def __init__(self, address, db=0, password=None, encoding=None, parser=None,
                 encoding_errors='strict', **kwargs):
        super(AsyncConnectionPoolBase, self).__init__(**kwargs)
        self.encoding = encoding or 'utf-8'
        self.encoding_errors = encoding_errors or 'strict'
        self.password = password
        self._setup(address, db, parser)
    
    def pubsub(self, shard_hint=None):
        return PubSub(self, shard_hint)
    
    def request(self, client, command_name, *args, **options):
        response = options.pop('consumer', None)
        full_response = options.pop('full_response', False)
        request = self._new_request(client, command_name, args, **options)
        response = self.response(request, response, False)
        return response if full_response else response.on_finished
    
    def request_pipeline(self, pipeline, raise_on_error=True):
        commands = pipeline.command_stack
        if not commands:
            return ()
        if pipeline.is_transaction:
            commands = list(chain([(('MULTI', ), {})], commands,
                                  [(('EXEC', ), {})]))
        request = self._new_request(pipeline, '', commands,
                                    raise_on_error=raise_on_error)
        return self.response(request).on_finished
    
    def _new_request(self, client, command_name, args, **options):
        return AsyncRedisRequest(client, self.connection, self.timeout,
                                 self.encoding, self.encoding_errors,
                                 command_name, args, self.redis_parser(),
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
        

class AsyncConnectionPool(AsyncConnectionPoolBase, RedisManager):
    pass


class CppAsyncConnectionPool(AsyncConnectionPoolBase, CppRedisManager):
    pass

        
class PubSub(pulsar.EventHandler):
    '''Implements :class:`PubSub` using a redis backend. To listen for
messages you can bind to the ``on_message`` event::

    from stdnet import getdb
    
    def handle_messages(channel_message):
        ...
        
    redis = getdb('redis://122.0.0.1:6379?timeout=0').client
    pubsub = redis.pubsub()
    pubsub.bind_event('on_message', handle_messages)
'''
    MANY_TIMES_EVENTS = ('on_message',)
    
    def __init__(self, connection_pool, shard_hint):
        super(PubSub, self).__init__()
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.consumer = None
        self._channels = set()
        self._patterns = set()
    
    @property
    def is_pipeline(self):
        return False
    
    def publish(self, channel, message):
        return self.execute_command('PUBLISH', channel, message)
    
    @async()
    def subscribe(self, *channels):
        channels, patterns = self._channel_patterns(channels)        
        if channels:
            yield self._execute('subscribe', *channels)
            self._channels.update(channels)
        if patterns:
            yield self._execute('psubscribe', *patterns)
            self._patterns.update(patterns)
        yield self._count_channels() 
    
    @async()
    def unsubscribe(self, *channels):
        channels, patterns = self._channel_patterns(channels)
        if not channels and not patterns:
            if self._channels:
                yield self._execute('unsubscribe')
                self._channels = set()
            if self._patterns:
                yield self._execute('punsubscribe')
                self._patterns = set()
        else:
            channels = self._channels.intersection(channels)
            patterns = self._patterns.intersection(patterns)
            if channels:
                yield self._execute('unsubscribe', *channels)
                self._channels.difference_update(channels)
            if patterns:
                yield self._execute('punsubscribe', *patterns)
                self._patterns.difference_update(patterns)
        yield self._count_channels()
            
    @async()
    def close(self):
        result = yield self.unsubscribe()
        if self.consumer:
            self.consumer.connection.close()
            self.consumer = None
        yield result
    
    def parse_response(self, connection, command_name):
        return connection.read_response()
        
    def on_response(self, result, raise_on_error):
        for response in result:
            if isinstance(response, Exception) and raise_on_error:
                raise response
            elif isinstance(response, list):
                command = response[0]
                if command == b'message':
                    response = response[1:3]
                    self.fire_event('on_message', response)
                elif command == b'pmessage':
                    response = response[2:4]
                    self.fire_event('on_message', response)
                elif command == b'unsubscribe' or command == b'punsubscribe':
                    response = response[2]
        return response
    
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        try:
            return self.connection_pool.request(self, *args, **options)
        except NoScriptError:
            self.connection_pool.clear_scripts()
            raise
        
    def _channel_patterns(self, channels):
        patterns = []
        simples = []
        for c in channels:
            if '*' in c:
                if c != '*':
                    patterns.append(c)
            else:
                simples.append(c)
        return simples, patterns
    
    def _count_channels(self):
        return len(self._channels) + len(self._patterns)
        
    def _execute(self, command, *args):
        if not self.consumer:
            # dummy request so we can obtain a connection
            req = self.connection_pool._new_request(self, '', ())
            connection = self.connection_pool.get_connection(req)
            self.consumer = self.connection_pool.consumer_factory(connection)
            self.consumer.release_connection = False
        on_finished = Deferred()
        self.execute_command(command, *args, consumer=self.consumer,
                             on_finished=on_finished)
        return on_finished
        