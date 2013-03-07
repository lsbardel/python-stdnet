'''Asynchronous connector for redis-py.

Usage::

    from pulsar.apps.redis import RedisClient
    
    client = RedisClient('127.0.0.1:6349')
    pong = yield client.ping()
'''
from collections import deque

import redis

import pulsar
from pulsar import ProtocolError, is_async, multi_async
from pulsar.utils.pep import ispy3k, map

from .extensions import get_script, RedisManager, RedisRequest    

__all__ = []


class AsyncRedisRequest(RedisRequest):
    
    def __init__(self, client, connection, timeout, encoding,
                 encoding_errors, command_name, args, options, reader):
        self.client = client
        self.connection = connection
        self.timeout = timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.command_name = command_name.upper()
        self.args = args
        self.options = options
        self.reader = reader
        if command_name:
            self.response = None
            self.command = self.pack_command(command_name, *args)
        else:
            self.response = []
            self.command = self.pack_pipeline(args)
        
    @property
    def key(self):
        return (self.connection, self.timeout)
    
    @property
    def address(self):
        return self.connection.address
    
    @property
    def is_pipeline(self):
        return isinstance(self.response, list)
    
    def __repr__(self):
        if self.command_name:
            return '%s%s' % (self.command_name, self.args)
        else:
            return 'PIPELINE{0}' % (self.args)
    __str__ = __repr__
    
    def read_response(self):
        # For compatibility with redis-py
        return self.response
    
    def feed(self, data):
        self.reader.feed(data)
        if self.is_pipeline:
            while 1:
                response = parser.gets()
                if response is False:
                    break
                self.response.append(response)
            if len(self.response) == self.num_responses:
                return self.close()
        else:
            self.response = self.reader.gets()
            if self.response is not False:
                return self.close()
    
    def close(self):
        self.fire_response(self.response)
        if isinstance(self.response, Exception):
            raise self.response
        return self.client.parse_response(self, self.command_name,
                                          **self.options)
    
    
class RedisProtocol(pulsar.ProtocolConsumer):
    
    def __init__(self, connection=None):
        super(RedisProtocol, self).__init__(connection=connection)
        self.all_requests = []
        
    def chain_request(self, request):
        self.all_requests.append(request)
        
    def new_request(self, request=None):
        if request is None:
            self._requests = deque(self.all_requests)
            request = self._requests.popleft()
        return super(RedisProtocol, self).new_request(request)
    
    def start_request(self):
        self.transport.write(self.current_request.command)
    
    def data_received(self, data):
        response = self.current_request.feed(data)
        if response is not None:
            # The request has finished
            if self._requests:
                self.new_request(self._requests.popleft())
            else:
                self.finished(response)
                
    
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
        request = self._new_request(client, command_name, *args, **options)
        return self.response(request)
    
    def request_pipeline(self, client, raise_on_error=True):
        if client.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline
        
    def response(self, request):
        connection = self.get_connection(request)
        consumer = self.consumer_factory(connection)
        # If this is a new connection we need to select database and login
        if not connection.processed:
            c = self.connection
            if self.password:
                req = self._new_request(request.client, 'auth', self.password)
                consumer.chain_request(req)
            if c.db:
                req = self._new_request(request.client, 'select', c.db)
                consumer.chain_request(req)
        consumer.chain_request(request)
        consumer.new_request()
        return consumer.on_finished
            
    def _new_request(self, client, command, *args, **options):
        return AsyncRedisRequest(client, self.connection, self.timeout,
                                 self.encoding, self.encoding_errors,
                                 command, args, options, self.redis_reader())
        
    def execute_script(self, client, to_load, callback):
        # Override execute_script so that we execute after scripts have loaded
        results = []
        for name in to_load:
            s = get_script(name)
            results.append(client.script_load(s.script))
        return multi_async(results).add_callback(callback)
        