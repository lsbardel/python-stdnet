'''Asynchronous Redis Connection for pulsar_ concurrent framework.

Requires pulsar_ concurrent framework.

.. _pulsar: http://packages.python.org/pulsar/
'''
from pulsar import AsyncIOStream, Deferred, make_async, async, is_async

from stdnet.lib.redis import connection, redis_before_send, NOT_READY


class RedisRequest(Deferred, connection.RedisRequest):
    
    def __init__(self, *args, **kwargs):
        Deferred.__init__(self)
        connection.RedisRequest.__init__(self, *args, **kwargs)
    
    def __str__(self):
        if self.command_name:
            if self.args:
                return '%s%s' % (self.command_name, self.args)
            else:
                return self.command_name
        else:
            return 'PIPELINE%s' % self.args
    __repr__ = __str__
    
    def execute(self):
        result = self.connection.connect(self)
        if is_async(result):
            result = result.add_callback(self.send)
        else:
            result = self.send()
        if is_async(result):
            result.add_callback(self.read_response)
        else:
            result = self.read_response()
        result.addBoth(self.callback)
        return self
    
    def _write(self, result=None):
        return self.connection.sock.write(self.command)

    @async(max_errors=1)    
    def read_response(self, result=None):
        response = NOT_READY
        sock = self.connection.sock
        while response is NOT_READY:
            rawdata = sock.read()
            if is_async(rawdata):
                yield rawdata
                rawdata = rawdata.result
            response = self.parse(rawdata)
        yield response
        
    @async(max_errors=1)
    def pool(self, num_messages=None):
        if not self.pooling:
            self.pooling = True
            yield self
            while self.pooling and self.connection.sock:
                yield self.read_response()
                if num_messages and count >= num_messages:
                    break
            self.pooling = False
                
                
class RedisConnection(connection.Connection):
    request_class = RedisRequest
    
    def _wrap_socket(self, sock):
        return AsyncIOStream(sock)
        
    def on_connect(self, request):
        "Initialize the connection, authenticate and select a database"
        client = request.client.client
        cmnd = None
        if self.password:
            cmnd = self.request(client, 'AUTH', self.password,
                                release_connection=False).execute()\
                       .add_errback(lambda r: self.connection_error(r,
                                                        'Invalid Password'))
        if self.db:
            cmnd = make_async(cmnd)
            cmnd.add_callback(
                lambda r: self.request(client, 'SELECT', self.db,
                                       release_connection=False).execute(),
                lambda r: self.connection_error(r, 'Invalid Database'))
        return cmnd
        
    def connection_error(self, failure, msg):
        raise ConnectionError(msg)