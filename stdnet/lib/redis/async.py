'''Asynchronous Redis Connection For pulsar_ concurrent framework.

Requires pulsar_

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
            result = result.add_callback(self._send)
        else:
            result = self._send()
        result.add_callback(self.callback)
        return self
        
    @async(max_errors=1)
    def _send(self, result=None):
        redis_before_send.send(self.client.__class__,
                               request=self,
                               command=self.command)
        sock = self.connection.sock
        yield sock.write(self.command)
        response = NOT_READY
        while response is NOT_READY:
            rawdata = sock.read()
            if is_async(rawdata):
                yield rawdata
                rawdata = rawdata.result
            response = self.parse(rawdata)
        yield response


class RedisConnection(connection.Connection):
    request_class = RedisRequest
    
    def _wrap_socket(self, sock):
        return AsyncIOStream(sock)
        
    def on_connect(self, request, counter):
        "Initialize the connection, authenticate and select a database"
        client = request.client.client
        cmnd = None
        if self.password:
            cmnd = self.execute_command(client, 'AUTH', self.password,
                                        release_connection=False)\
                       .add_errback(lambda r: self.connection_error(r,
                                                        'Invalid Password'))
        if self.db:
            cmnd = make_async(cmnd)
            cmnd.add_callback(
                lambda r: self.execute_command(
                        client, 'SELECT', self.db, release_connection=False),
                lambda r: self.connection_error(r, 'Invalid Database'))
        return cmnd
        
    def connection_error(self, failure, msg):
        raise ConnectionError(msg)
    