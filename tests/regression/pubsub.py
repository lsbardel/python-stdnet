from stdnet import test
from stdnet.apps.pubsub import Publisher, Subscriber


class TestPubSub(test.TestCase):
    
    def testPublisher(self):
        p = Publisher()
        self.assertTrue(p.pickler)
        self.assertTrue(p.client)
        
    def testSimple(self):
        s = Subscriber()
        p = Publisher()
        # subscribe to 'test' message queue
        s.subscribe('test')
        self.assertEqual(s.subscription_count(), 1)
        self.assertEqual(p.publish('test','hello world!'), 1)
        res = list(s.pull(count = 1))
        self.assertEqual(len(res),1)
        res = res[0]
        self.assertEqual(res['data'],'hello world!')
        