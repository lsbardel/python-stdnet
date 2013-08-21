'''Tests asynchronous PubSub.'''
import pulsar

from stdnet.utils import test
from stdnet.utils.async import async_binding


class Listener(pulsar.Deferred):
    count = 0
    def __init__(self, channel, size):
        super(Listener, self).__init__()
        self.channel = channel
        self.size = size
        self.messages = []
        
    def on_message(self, channel_message):
        channel, message = channel_message
        if self.channel == channel.decode('utf-8'):
            self.count += 1
            self.messages.append(message)
            if self.count == self.size:
                self.callback(self.messages)
            

@test.skipUnless(async_binding, 'Requires asynchronous binding')
class TestRedisPrefixed(test.TestCase):
    multipledb = 'redis'
        
    @classmethod
    def backend_params(cls):
        return {'timeout': 0}
        
    def test_subscribe_one(self):
        pubsub = self.backend.client.pubsub()
        self.assertFalse(pubsub.consumer)
        # Subscribe to one channel
        channels = yield pubsub.subscribe('blaaaaaa')
        self.assertTrue(pubsub.consumer)
        self.assertEqual(channels, 1)
        channels = yield pubsub.subscribe('blaaaaaa')
        self.assertEqual(channels, 1)
        channels = yield pubsub.subscribe('foooo', 'jhkjhkjhkh')
        self.assertEqual(channels, 3)
    
    def test_subscribe_many(self):
        pubsub = self.backend.client.pubsub()
        self.assertFalse(pubsub.consumer)
        # Subscribe to one channel
        channels = yield pubsub.subscribe('blaaaaaa', 'foooo', 'hhhh', 'bla.*')
        self.assertTrue(pubsub.consumer)
        self.assertEqual(channels, 4)
        channels = yield pubsub.unsubscribe('blaaaaaa', 'foooo')
        self.assertEqual(channels, 2)
        channels = yield pubsub.unsubscribe()
        self.assertEqual(channels, 0)
       
    def test_unsubscribe(self):
        p = self.backend.client.pubsub()
        channels = yield p.subscribe('blaaa.*', 'fooo', 'hhhhhh')
        self.assertEqual(channels, 3)
        channels = yield p.subscribe('hhhhhh')
        self.assertEqual(channels, 3)
        #
        # Now unsubscribe
        channels = yield p.unsubscribe('blaaa.*')
        self.assertEqual(channels, 2)
        channels = yield p.unsubscribe('blaaa.*')
        self.assertEqual(channels, 2)
        channels = yield p.unsubscribe()
        self.assertEqual(channels, 0)
    
    def test_publish(self):
        pubsub = self.backend.client.pubsub()
        pubsub.subscribe('bla')
        result = yield pubsub.publish('bla', 'Hello')
        self.assertTrue(result>=0)
        
    def test_count_messages(self):
        pubsub = self.backend.client.pubsub()
        pubsub.subscribe('counting')
        listener = Listener('counting', 2)
        pubsub.bind_event('on_message', listener.on_message)
        result = yield pubsub.publish('counting', 'Hello')
        self.assertTrue(result>=0)
        pubsub.publish('counting', 'done')
        result = yield listener
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result), set((b'Hello', b'done')))
        
    def test_count_messages4(self):
        pubsub = self.backend.client.pubsub()
        yield pubsub.subscribe('close')
        self.assertEqual(len(pubsub.channels), 1)
        listener = Listener('close', 4)
        pubsub.bind_event('on_message', listener.on_message)
        pubsub.publish('close', 'Hello')
        pubsub.publish('close', 'Hello2')
        pubsub.publish('close', 'Hello3')
        pubsub.publish('close', 'done')
        result = yield listener
        self.assertEqual(len(result), 4)
        self.assertEqual(set(result),
                         set((b'Hello', b'Hello2', b'Hello3', b'done')))