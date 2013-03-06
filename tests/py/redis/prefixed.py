
from .base import TestCase


class TestRedis(TestCase):
    
    def test_coverage(self):
        c = self.backend.client
        self.assertEqual(c.prefix, '')
        self.assertTrue(c.dbsize() >= 0)
    
    
class TestRedisPrefixed(TestCase):
    
    def get_client2(self,):
        return self.backend.client.prefixed('xxx' + self.namespace)
    
    def setUp(self):
        self.client2 = self.get_client2()
        yield self.client2.flushdb()
        yield super(TestRedisPrefixed, self).setUp()
    
    def tearDown(self):
        yield self.client2.flushdb()
        yield super(TestRedisPrefixed, self).tearDown()
    
    def test_delete(self):
        yield self.client.set('bla', 'foo')
        yield self.client2.set('bla', 'foo')
        yield self.async.assertEqual(self.client.dbsize(), 1)
        yield self.async.assertEqual(self.client2.dbsize(), 1)
        yield self.client.flushdb()
        yield self.assertEqual(self.client.dbsize(), 0)
        yield self.assertEqual(self.client2.dbsize(), 1)
        yield self.client2.flushdb()
        yield self.assertEqual(self.client2.dbsize(), 0)
        
    def test_error(self):
        self.assertRaises(NotImplementedError,
                          self.client.execute_command, 'FLUSHDB')
        self.assertRaises(NotImplementedError,
                          self.client.execute_command, 'FLUSHALL')
        
        
    