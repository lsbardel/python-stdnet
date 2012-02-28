from stdnet import test
from stdnet.apps.pubsub import Publisher, Subscriber


class TestPubSub(test.TestCase):
    
    def setUp(self):
        self.s = Subscriber()
        
    def __tearDown(self):
        self.s.unsubscribe()
        self.s.punsubscribe()
        
    def __testPublisher(self):
        p = Publisher()
        self.assertTrue(p.pickler)
        self.assertTrue(p.client)
        
    def __testSimple(self):
        s = self.s
        p = Publisher()
        # subscribe to 'test' message queue
        s.subscribe('test')
        self.assertEqual(s.subscription_count(), 1)
        self.assertEqual(p.publish('test','hello world!'), 1)
        res = list(s.pull(count = 1))
        self.assertEqual(len(res),1)
        res = res[0]
        self.assertEqual(res['data'],'hello world!')
        