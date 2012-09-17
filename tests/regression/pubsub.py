import os

from stdnet import test, getdb
from stdnet.apps.pubsub import Publisher, Subscriber

from .backend import DummyBackendDataServer


@test.skipUnless(os.environ['stdnet_test_suite'] == 'pulsar', 'Requires Pulsar')
class TestPubSub(test.TestCase):
    
    def setUp(self):
        self.s = Subscriber()
    
    #def tearDown(self):
    #    self.s.unsubscribe()
    #    self.s.punsubscribe()
    
    def publisher(self):
        p = Publisher(self.backend)
        self.assertTrue(p.pickler)
        self.assertTrue(p.server)
        return p
        
    def subscriber(self):
        from stdnet.lib.redis.async import RedisConnection
        b = getdb(self.backend.connection_string,
                  connection_class=RedisConnection)
        s = Subscriber(b)
        self.assertTrue(s.server)
        return s
    
    def testDummy(self):
        p = Publisher(DummyBackendDataServer('',''))
        self.assertRaises(NotImplementedError, p.publish, '', '')
        
    def testClasses(self):
        p = self.publisher()
        s = self.subscriber()
        
    def testSimple(self):
        p = self.publisher()
        s = self.subscriber()
        yield s.subscribe('test')
        self.assertEqual(s.subscription_count(), 1)
        self.assertEqual(p.publish('test','hello world!'), 1)
        res = list(s.pool())
        self.assertEqual(len(res),1)
        res = res[0]
        self.assertEqual(res['data'],'hello world!')
        