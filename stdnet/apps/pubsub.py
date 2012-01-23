from inspect import isclass

from stdnet import getdb
from stdnet.utils.encoders import Json
from stdnet.utils import is_string
from stdnet.lib.exceptions import ConnectionError


class PubSub(object):
    
    def __init__(self, server = None, pickler = Json, shard_hint = None):
        if isclass(pickler):
            pickler = pickler()
        self.pickler = pickler
        self.client = getdb(server).client
    
    
class Publisher(PubSub):
    
    def publish(self, channel, data):
        data = self.pickler.dumps(data)
        return self.client.execute_command('PUBLISH', channel, data)
    
    
class Subscriber(PubSub):
    
    def __init__(self, *args, **kwargs):
        super(Subscriber,self).__init__(*args, **kwargs)
        self.shard_hint = kwargs.get('shard_hint')
        self.connection = None
        self.subscription_count = 0
        self.channels = set()
        self.patterns = set()
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
        if self.connection:
            self.client.connection_pool.release(self.connection)
            self.connection = None
            
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
    
    def execute_command(self, command, channels, container, add = True):
        "Execute a publish/subscribe command"
        if is_string(channels):
            channels = [channels]
        if add:
            for c in channels:
                container.add(c)
        else:
            for c in channels:
                try:
                    container.remove(c)
                except KeyError:
                    pass
        if self.connection is None:
            self.connection = self.client.connection_pool.get_connection(
                'pubsub',
                self.shard_hint
                )
        connection = self.connection
        try:
            connection.send_command(command, *channels)
            return self.parse_response()
        except ConnectionError:
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

    def parse_response(self):
        "Parse the response from a publish/subscribe command"
        response = self.connection.read_response()
        if response[0] in self.subscribe_commands:
            self.subscription_count = response[2]
            # if we've just unsubscribed from the remaining channels,
            # release the connection back to the pool
            if not self.subscription_count:
                self.disconnect()
        #data = self.pickler.dumps(data)
        return response
    
    def pull(self, timeout = None, count = None):
        c = 0
        loads = self.pickler.loads
        while self.subscription_count and (count and c < count):
            r = self.parse_response()
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
    