'''A `Publish/Subscribe message paradigm`__ where (citing Wikipedia) senders
(publishers) are not programmed to send their messages to specific receivers
(subscribers).
It is currently only available for :ref:`Redis backend <redis-server>`.

__ http://en.wikipedia.org/wiki/Publish/subscribe

API
======

The Publish/Subscribe application exposes two classes, one for publishing
messages and one for subscribing to channels. Subscribers express interest
in one or more channels, and only receive messages that are of interest,
without knowledge of what (if any) publishers there are.
This decoupling of publishers and subscribers can allow for greater
scalability and a more dynamic network topology.

Publisher
~~~~~~~~~~~~

.. autoclass:: Publisher
   :members:
   :member-order: bysource

Subscriber
~~~~~~~~~~~~

.. autoclass:: Subscriber
   :members:
   :member-order: bysource
'''
import logging
from inspect import isclass

from stdnet import getdb, AsyncObject
from stdnet.utils.encoders import Json
from stdnet.utils import is_string


logger = logging.getLogger('stdnet.pubsub')
    

class PubSub(object):
    pickler = Json
    
    def __init__(self, server=None, pickler=None):
        pickler = pickler or self.pickler
        if isclass(pickler):
            pickler = pickler()
        self.pickler = pickler
        self.server = getdb(server)
        
        
class Publisher(PubSub):
    '''A publisher of messages to channels.
    
.. attribute:: server

    The :class:`stdnet.BackendDataServer` which publishes messages.
    
'''
    def publish(self, channel, message):
        '''Publish a new ``message`` to ``channel``.'''
        message = self.pickler.dumps(message)
        return self.server.publish(channel, message)
        
        
class Subscriber(PubSub):
    '''A subscriber to channels.
    
.. attribute:: server

    :class:`stdnet.BackendDataServer` which subscribes to channels.
    
.. attribute:: channels

    Dictionary of channels messages. this is a (potentially) nested
    dictionary when the :class:`Subscriber` subscribes to a
    pattern matching collection of channels. 
'''    
    def __init__(self, server=None, pickler=None):
        super(Subscriber, self).__init__(server, pickler)
        self.channels = {}
        self._subscriber = self.server.subscriber(
                                message_callback=self.message_callback)
        
    def disconnect(self):
        '''Stop listening for messages from :attr:`channels`.'''
        self._subscriber.disconnect()
    
    def subscription_count(self):
        return self._subscriber.subscription_count()
    
    def subscribe(self, *channels):
        return self._subscriber.subscribe(self._channel_list(channels))
     
    def unsubscribe(self, *channels):
        return self._subscriber.unsubscribe(self._channel_list(channels))
    
    def psubscribe(self, *channels):
        return self._subscriber.psubscribe(self._channel_list(channels))
    
    def punsubscribe(self, *channels):
        return self._subscriber.punsubscribe(self._channel_list(channels))

    def message_callback(self, command, channel, msg=None, sub_channel=None):
        if command == 'subscribe':
            self.channels[channel] = []
        elif command == 'unsubscribe':
            self.channels.pop(channel, None)
        elif channel in self.channels:
            ch = self.channels[channel]
            if sub_channel:
                if not isinstance(ch, dict):
                    ch = {}
                    self.channels[channel] = ch
                if sub_channel not in ch:
                    ch[sub_channel] = []
                ch = ch[sub_channel]
            ch.append(self.pickler.loads(msg))
        else:
            logger.warn('Got message for unsubscribed channel "%s"' % channel)
    
    def get_all(self, channel=None):
        if channel is None:
            channels = {}
            for channel in self.channels:
                data = self.channels[channel]
                if data:
                    channels[channel] = data
                    self.channels[channel] = []
            return channels
        elif channel in self.channels:
            data = self.channels[channel]
            self.channels[channel] = []
            return data
        
    def pool(self, num_messages=None):
        '''Pull data from subscribed channels.
        
:param timeout: Pool timeout in seconds'''
        return self._subscriber.pool(num_messages)
    
    # PRIVATE METHODS
    
    def _channel_list(self, channels):
        ch = []
        for channel in channels:
            if not isinstance(channel, (list, tuple)):
                ch.append(channel)
            else:
                ch.extend(channel)
        return ch
