from inspect import isclass

from stdnet import getdb
from stdnet.utils.encoders import Json
from stdnet.utils import is_string

from .client import RedisProxy
    

__all__ = ['Publisher','Subscriber']
    
class Publisher(object):
    '''Class which publish messages to message queues.'''
    def __init__(self, server = None, pickler = Json):
        if isclass(pickler):
            pickler = pickler()
        self.pickler = pickler
        self.client = getdb(server).client
        
    def publish(self, channel, data):
        data = self.pickler.dumps(data)
        #return self.backend.publish(channel, data)
        return self.client.execute_command('PUBLISH', channel, data)
    
    
class Subscriber(RedisProxy):
    '''Subscribe to '''
    def __init__(self, client):
        super(Subscriber,self).__init__(client)
        self.connection = None
        self._subscription_count = 0
        self._request = None
        self.channels = set()
        self.patterns = set()
        self.options = {'release_connection': False}
        self.subscribe_commands = set(
            (b'subscribe', b'psubscribe', b'unsubscribe', b'punsubscribe')
            )
        
    def __del__(self):
        try:
            if self.connection and (self.channels or self.patterns):
                self.connection.disconnect()
            self.disconnect()
        except:
            pass
        
    def disconnect(self):
        if self.connection is not None:
            self.client.connection_pool.release(self.connection)
            self.connection = None
            
    def subscription_count(self):
        return self._subscription_count
    
    def subscribe(self, channels):
        return self.execute_command('SUBSCRIBE', channels, self.channels)
     
    def unsubscribe(self, channels):
        return self.execute_command('UNSUBSCRIBE', channels, self.channels,
                                    False)
    
    def psubscribe(self, channels):
        return self.execute_command('PSUBSCRIBE', channels, self.patterns)
    
    def punsubscribe(self, channels):
        return self.execute_command('PUNSUBSCRIBE', channels, self.patterns,
                                    False)
    
    def request(self):
        if self._request is None:
            if self.connection is None:
                raise ValueErrior('No connection')
            self._request = self.connection.request_class(self.client,
                                        self.connection, False, (),
                                        release_connection = False)
        return self._request
    
    def execute_command(self, command, channels, container, add = True):
        "Internal function which execute a publish/subscribe command."
        channels = channels or ()
        if is_string(channels):
            channels = [channels]
        if add:
            for c in channels:
                container.add(c)
        else:
            if not channels:
                container.clear()
            else:
                for c in channels:
                    try:
                        container.remove(c)
                    except KeyError:
                        pass
        if self.connection is None:
            self.connection = self.client.connection_pool.get_connection()
        connection = self.connection
        try:
            return connection.execute_command(self, command,
                                              *channels, **self.options)
        except redis.ConnectionError:
            connection.disconnect()
            # Connect manually here. If the Redis server is down, this will
            # fail and raise a ConnectionError as desired.
            connection.connect()
            # resubscribe to all channels and patterns before
            # resending the current command
            for channel in self.channels:
                self.subscribe(channel)
            for pattern in self.patterns:
                self.psubscribe(pattern)
            connection.send_command(command, channels)
            return self.parse_response()

    def parse_response(self, request):
        "Parse the response from a publish/subscribe command"
        response = request.response
        if response[0] in self.subscribe_commands:
            self._subscription_count = response[2]
            # if we've just unsubscribed from the remaining channels,
            # release the connection back to the pool
            if not self._subscription_count:
                self.disconnect()
        #data = self.pickler.dumps(data)
        return response
    
    def pull(self, timeout, count, loads):
        '''Retrieve new messages from the subscribed channels.

:parameter timeout: a timeout in seconds'''
        c = 0
        request = self.request()
        while self.subscription_count and (count and c < count):
            r = request.read_response()
            c += 1
            if r[0] == b'pmessage':
                msg = {
                    'type': 'pmessage',
                    'pattern': r[1].decode('utf-8'),
                    'channel': r[2].decode('utf-8'),
                    'data': loads(r[3])
                }
            else:
                msg = {
                    'type': 'message',
                    'pattern': None,
                    'channel': r[1].decode('utf-8'),
                    'data': loads(r[2])
                }
            yield msg
