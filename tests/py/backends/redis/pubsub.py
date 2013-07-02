from stdnet.utils import test
from stdnet.utils.async import async_binding


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
        channels = yield pubsub.subscribe('foooo')
        self.assertEqual(channels, 2)
    
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
        import pulsar
        pubsub = self.backend.client.pubsub()
        pubsub.subscribe('counting')
        
        class Listener(pulsar.Deferred):
            count = 0
            def on_message(self, channel_message):
                self.count += 1
                _, message = channel_message
                if message == 'done':
                    self.callback('done')
                
        listener = Listener()
        pubsub.bind_event('on_message', listener.on_message)
        result = yield pubsub.publish('counting', 'Hello')
        self.assertTrue(result>=0)
        pubsub.publish('counting', 'done')
        yield listener
        self.assertEqual(listener.count, 2)
        