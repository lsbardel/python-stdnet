from gevent.socket import socket, error
from gevent import coros, spawn

from stdnet.lib.redis import connection

from .async import Deferred, Failure


class RedisRequest(connection.SyncRedisRequest, Deferred):
    
    def __init__(self, *args, **kwargs):
        Deferred.__init__(self)
        super(RedisRequest, self).__init__(*args, **kwargs)
        
    def execute(self):
        execute = super(RedisRequest, self).execute
        self.connection._request = self
        self.greenlet = spawn(execute)
        self.greenlet.link(self._finished)
        return self
    
    def _finished(self, greenlet):
        if self.greenlet.exception:
            result = Failure(greenlet.exception)
        else:
            result = greenlet.value
        return self.callback(result)

class Connection(connection.Connection):
    request_class = RedisRequest
    socket_class = socket
    