from inspect import isclass

from stdnet import getdb, AsyncObject
from stdnet.utils.encoders import Json
from stdnet.utils import is_string
    
    
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
    
    
class Subscriber(AsyncObject):
    '''Subscribe to '''
    def __init__(self, server = None, pickler = Json):
        if isclass(pickler):
            pickler = pickler()
        self.pickler = pickler
        self.client = getdb(server).subscriber()
        
    def disconnect(self):
        self.client.disconnect()
    
    def subscription_count(self):
        return self.client.subscription_count()
    
    def subscribe(self, channels):
        return self.client.subscribe(channels)
     
    def unsubscribe(self, channels = None):
        return self.client.unsubscribe(channels)
    
    def psubscribe(self, channels):
        return self.client.psubscribe(channels)
    
    def punsubscribe(self, channels = None):
        return self.client.punsubscribe(channels)
    
    def pull(self, timeout = None, count = None):
        '''Retrieve new messages from the subscribed channels.

:parameter timeout: Optional timeout in seconds.
:parameter count: Optional number of messages to retrieve.'''
        return self.client.pull(timeout, count, self.pickler.loads)
       