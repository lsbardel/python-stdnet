from collections import deque

from stdnet.utils import is_string

from .client import RedisProxy, RedisConnectionTimeout
    

__all__ = ['Subscriber']
    
    
class Subscriber(RedisProxy):
    '''Subscriber'''
    subscribe_commands = frozenset(('subscribe', 'psubscribe'))
    unsubscribe_commands = frozenset(('unsubscribe', 'punsubscribe'))
    message_commands = frozenset(('message', 'pmessage'))
    request = None
    
    def __init__(self, client, message_callback=None):
        super(Subscriber,self).__init__(client)
        self.message_callback = message_callback
        self._subscription_count = 0
        
    def __del__(self):
        self.disconnect()
        
    def disconnect(self):
        if self.request is not None:
            self._subscription_count = 0
            self.request.connection.disconnect()
            self.request = None
            
    def subscription_count(self):
        return self._subscription_count
    
    def subscribe(self, channels):
        return self.execute_command('subscribe', channels)
     
    def unsubscribe(self, channels):
        return self.execute_command('unsubscribe', channels)
    
    def psubscribe(self, channels):
        return self.execute_command('psubscribe', channels)
    
    def punsubscribe(self, channels):
        return self.execute_command('punsubscribe', channels)
    
    def execute_command(self, command, channels):
        command, channels = (command, channels) 
        if self.request is None:
            if command in self.subscribe_commands:
                connection = self.connection_pool.get_connection()
                self.request = connection.request(self, command, *channels,
                                                  release_connection=False)
                return self.request.execute()
        if self.request:
            c = self.request.connection
            return c.request(self, command, *channels,
                             release_connection=False).send()
    
    def pool(self, num_messages):
        return self.request.pool(num_messages)
    
    def parse_response(self, request):
        "Parse the response from a publish/subscribe command"
        response = request.response
        command, channel = [r.decode() for r in response[:2]]
        if command in self.subscribe_commands:
            self.message_callback('subscribe', channel)
            self._subscription_count = response[2]
        elif command in self.unsubscribe_commands:
            self.message_callback('unsubscribe', channel)
            self._subscription_count = response[2]
        elif command in self.message_commands:
            msg = tuple(reversed([r.decode() for r in response[2:]]))
            self.message_callback('message', channel, *msg)
        if not self._subscription_count:
            self.disconnect()
        return response