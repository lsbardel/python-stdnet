from functools import partial

from pulsar import AsyncIOStream, Deferred

from .exceptions import ConnectionError
from . import connection



class AsyncRedisRequest(connection.RedisRequest, Deferred):
    
    def __init__(self, *args, **kwargs):
        Deferred.__init__(self)
        connection.RedisRequest.__init__(self, *args, **kwargs)
        
    def send(self, command):
        c = self.connection.connect()
        if c:
            c.add_callback(lambda r : self._send(command,r))
        else:
            self._send(command)
        
    def _send(self, command, conn_result = None):
        return self.connection.stream.write(command)\
                   .add_callback(self.callback)
    
    def add_errback(self, expected, error):
        return self.add_callback(partial(self.check_result,
                                         expected,error))
    
    def check_result(self, expected, error, result):
        if result != expected:
            raise error
        return result


class AsyncRedisConnection(connection.Connection):
    blocking = False
    request_class = AsyncRedisRequest
    
    def async_connect(self, sock):
        self._sock = sock
        self.stream = AsyncIOStream(sock)
        return self.stream.connect(self.address, self.on_connect)
        
    def on_connect(self, result = None):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)

        # if a password is specified, authenticate
        r = None
        if self.password:
            r = self.request('AUTH', self.password)\
                        .add_errback('OK',ConnectionError('Invalid Password'))
            #if self.read_response() != 'OK':
            #    raise ConnectionError('Invalid Password')

        # if a database is specified, switch to it
        if self.db:
            return r.add_callback(self.select) if r else self.select()
        else:
            return r
        
    def select(self, result = None):
        return self.request('SELECT', self.db)\
                .add_errback('OK', ConnectionError('Invalid Database'))
        