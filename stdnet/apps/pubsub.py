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

from stdnet import odm, getdb, AsyncObject
from stdnet.utils import encoders


logger = logging.getLogger('stdnet.pubsub')
    

def publish_model(model, data=None):
    '''Publish ``data`` on the ``model`` channel.'''
    backend = model.objects.backend
    if isinstance(model, odm.StdModel) and data is None:
        data = model
    channel = backend.basekey(model._meta)
    return Publisher(backend).publish(channel, data)
    
    
class PubSub(object):
    encoder = encoders.Json
    
    def __init__(self, server=None, encoder=None):
        encoder = encoder or self.encoder
        if isclass(encoder):
            encoder = encoder()
        self.encoder = encoder
        self.server = getdb(server)
        
        
class Publisher(PubSub):
    '''A publisher of messages to channels.
    
.. attribute:: server

    The :class:`stdnet.BackendDataServer` which publishes messages.
    
.. attribute:: encoder

    The :class:`stdnet.utils.encoders.Encoder` to encode messages.
    If not provided the :class:`stdnet.utils.encoders.Json` encoder is used.
'''
    def publish(self, channel, message):
        '''Publish a new ``message`` to ``channel``.'''
        message = self.encoder.dumps(message)
        return self.server.publish(channel, message)
        
        
class Subscriber(PubSub):
    '''A subscriber to channels.
    
.. attribute:: server

    :class:`stdnet.BackendDataServer` which subscribes to channels.

.. attribute:: encoder

    The :class:`stdnet.utils.encoders.Encoder` to decode messages.
    If not provided the :class:`stdnet.utils.encoders.Json` encoder is used.
        
.. attribute:: channels

    Dictionary of channels messages. this is a (potentially) nested
    dictionary when the :class:`Subscriber` subscribes to a
    pattern matching collection of channels.
    
**METHODS**
'''    
    def __init__(self, server=None, encoder=None, on_message=None):
        super(Subscriber, self).__init__(server, encoder)
        self.channels = {}
        if on_message:
            self.on_message = on_message
        self._subscriber = self.server.subscriber(
                                message_callback=self.message_callback)
        
    def disconnect(self):
        '''Stop listening for messages from :attr:`channels`.'''
        self._subscriber.disconnect()
    
    def subscription_count(self):
        return self._subscriber.subscription_count()
    
    def subscribe(self, *channels):
        '''Subscribe to a list of ``channels``.'''
        return self._subscriber.subscribe(self._channel_list(channels))
     
    def unsubscribe(self, *channels):
        '''Unsubscribe from a list of ``channels``.'''
        return self._subscriber.unsubscribe(self._channel_list(channels))
    
    def psubscribe(self, *channels):
        return self._subscriber.psubscribe(self._channel_list(channels))
    
    def punsubscribe(self, *channels):
        return self._subscriber.punsubscribe(self._channel_list(channels))

    def poll(self, num_messages=None, timeout=None):
        '''Pull data from subscribed channels.

:param num_messages: Number of messages to poll. If ``None`` keep on polling
    indefinetly or until *timeout* is reached.
:param timeout: Pool timeout in seconds.'''
        return self._subscriber.poll(num_messages=num_messages, timeout=timeout)
    
    def on_message(self, message, channel=None, sub_channel=None, **kwargs):
        '''A callback invoked every time a new message is available
on a channel. It is a function accepting three parameters::'''
        ch = self.channels[channel]
        if sub_channel:
            if not isinstance(ch, dict):
                ch = {}
                self.channels[channel] = ch
            if sub_channel not in ch:
                ch[sub_channel] = []
            ch = ch[sub_channel]
        ch.append(message)
            
    def message_callback(self, command, channel, msg=None, sub_channel=None):
        # The callback from the _subscriber implementation when new data
        # is available.
        if command == 'subscribe':
            self.channels[channel] = []
        elif command == 'unsubscribe':
            self.channels.pop(channel, None)
        elif channel in self.channels:
            msg = self.encoder.loads(msg)
            return self.on_message(msg, channel=channel,
                                   sub_channel=sub_channel,
                                   subscriber=self)
        else:
            logger.warn('Got message for unsubscribed channel "%s"', channel)
    
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
    
    # PRIVATE METHODS
    
    def _channel_list(self, channels):
        ch = []
        for channel in channels:
            if not isinstance(channel, (list, tuple)):
                ch.append(channel)
            else:
                ch.extend(channel)
        return ch
