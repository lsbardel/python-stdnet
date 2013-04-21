from stdnet.utils import test

    
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
    
    def test_publish(self):
        pubsub = self.backend.client.pubsub()
        result = yield pubsub.publish('bla', 'Hello')
        self.assertTrue(result>=0)