
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
        self.client2.flushdb()
        return super(TestRedisPrefixed, self).setUp()
    
    def tearDown(self):
        self.client2.flushdb()
        return super(TestRedisPrefixed, self).tearDown()
    
    def test_delete(self):
        self.client.set('bla', 'foo')
        self.client2.set('bla', 'foo')
        self.assertEqual(self.client.dbsize(), 1)
        self.assertEqual(self.client2.dbsize(), 1)
        self.client.flushdb()
        self.assertEqual(self.client.dbsize(), 0)
        self.assertEqual(self.client2.dbsize(), 1)
        self.client2.flushdb()
        self.assertEqual(self.client2.dbsize(), 0)
        
    def testError(self):
        self.assertRaises(NotImplementedError,
                          self.client.execute_command, 'FLUSHDB')
        self.assertRaises(NotImplementedError,
                          self.client.execute_command, 'FLUSHALL')
        
        
    