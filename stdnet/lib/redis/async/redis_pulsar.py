'''Asynchronous Redis Connection For pulsar_ concurrent framework.

Requires pulsar_

.. _pulsar: http://packages.python.org/pulsar/
'''
from functools import partial

from pulsar import AsyncIOStream, Deferred

from stdnet.lib.redis import connection


class RedisRequest(connection.RedisRequest, Deferred):
    
    def __init__(self, *args, **kwargs):
        Deferred.__init__(self)
        super(RedisRequest,self).__init__(self, *args, **kwargs)
        
    def send(self):
        c = self.connection.connect(self)
        if c:
            return c.add_callback(lambda result : self._send(), True)
        else:
            return self._send()
        
    def _send(self):
        stream = self.connection.stream
        return stream.write(self.command,
                    lambda num_bytes : stream.read(self.parse))
                   
    def close(self):
        super(AsyncRedisRequest,self).close()
        self.callback(self._response)
    
    def add_errback(self, expected, error):
        return self.add_callback(partial(self.check_result,
                                         expected,error), True)
    
    def check_result(self, expected, error, result):
        if result != expected:
            raise error
        return result
    
    def execute(self):
        self.send()
        return self


class Connection(connection.Connection):
    request_class = RedisRequest
    
    def _connect(self, request, counter):
        self.stream = AsyncIOStream(self.sock)
        return self.stream.connect(self.address,
                                partial(self.on_connect,request,counter))
        
    def on_connect(self, request, counter, result=None):
        "Initialize the connection, authenticate and select a database"
        # if a password is specified, authenticate
        r = self._auth(request) if self.password else None
        # if a database is specified, switch to it
        if self.db:
            return r.add_callback(partial(self._select,request)) if r\
                     else self._select(request)
        else:
            return r
        
    def _auth(self, request):
        client = request.client.client
        return self.execute_command(client, 'AUTH', self.password,
                                    release_connection = False)\
                .add_errback(True, ConnectionError('Invalid Password'))
                
    def _select(self, request, result = None):
        client = request.client.client
        return self.execute_command(client, 'SELECT', self.db,
                                    release_connection = False)\
                   .add_errback(True, ConnectionError('Invalid Database'))
        