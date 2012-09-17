from collections import deque

from stdnet.utils import is_string

from .client import RedisProxy
    

__all__ = ['Subscriber']
    
    
class Subscriber(RedisProxy):
    '''Subscriber'''
    subscribe_commands = frozenset(('subscribe', 'psubscribe'))
    unsubscribe_commands = frozenset(('unsubscribe', 'punsubscribe'))
    message_commands = frozenset(('message', 'pmessage'))
    connection = None
    request = None
    
    def __init__(self, client, message_callback=None):
        super(Subscriber,self).__init__(client)
        self.command_queue = deque()
        self.message_callback = message_callback
        self._subscription_count = 0
        
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
    
    def pool(self):
        if self.request:
            return self.request.read_response()
                
    # INTERNALS
    
    def execute_command(self, command, channels):
        cmd = (command, channels) 
        if self.connection is None:
            if command in self.subscribe_commands:
                self.connection = self.client.connection_pool.get_connection()
                self.command_queue.append(cmd)
                return self._execute_next()
        else:
            self.command_queue.append(cmd)
            return cmd
        
    def _execute_next(self):
        if self.command_queue:
            command, channels = self.command_queue.popleft()
            return self.connection.execute_command(self, command, *channels)
    
    def parse_response(self, request):
        "Parse the response from a publish/subscribe command"
        self.request = request
        response = request.response
        #request.connection.streaming = True
        #request.command = None
        command = response[0].decode()
        if command in self.subscribe_commands:
            self.message_callback('subscribe', response[1].decode())
            self._subscription_count = response[2]
        elif command in self.unsubscribe_commands:
            self.message_callback('unsubscribe', response[1].decode())
            self._subscription_count = response[2]
        elif command in self.message_commands:
            self.message_callback('message', self.get_message(response))
        if not self._subscription_count:
            self.disconnect()
        return response
    
    def get_message(self, response):
        return response