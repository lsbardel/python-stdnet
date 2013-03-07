from . import client

    
class TestRedisPrefixed(client.TestCase):
    
    def get_client2(self, prefix='xxx'):
        return self.backend.client.prefixed(prefix + self.namespace)
    
    def setUp(self):
        self.client2 = self.get_client2()
        yield self.client2.flushdb()
        yield super(TestRedisPrefixed, self).setUp()
    
    def tearDown(self):
        yield self.client2.flushdb()
        yield super(TestRedisPrefixed, self).tearDown()
    
    def test_meta(self):
        c = self.get_client2('yyy')
        self.assertTrue(c.prefix)
        self.assertTrue(c.prefix.startswith('yyy'))
        self.assertTrue(c.client)
        self.assertFalse(c.client.prefix)
        
    def test_delete(self):
        yield self.client.set('bla', 'foo')
        yield self.client2.set('bla', 'foo')
        yield self.async.assertEqual(self.client.dbsize(), 1)
        yield self.async.assertEqual(self.client2.dbsize(), 1)
        yield self.client.flushdb()
        yield self.async.assertEqual(self.client.dbsize(), 0)
        yield self.async.assertEqual(self.client2.dbsize(), 1)
        yield self.client2.flushdb()
        yield self.async.assertEqual(self.client2.dbsize(), 0)
        
    def test_error(self):
        self.assertRaises(NotImplementedError,
                          self.client.execute_command, 'FLUSHDB')
        self.assertRaises(NotImplementedError,
                          self.client.execute_command, 'FLUSHALL')
        
        
    