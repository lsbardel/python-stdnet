from stdnet.utils import test
from stdnet.utils import gen_unique_id

    
class TestRedisPrefixed(test.TestCase):
    multipledb = 'redis'
    
    def get_client(self, prefix=None):
        prefix = prefix or gen_unique_id()
        c = self.backend.client.prefixed(prefix + self.namespace)
        if c.prefix not in self.clients:
            self.clients[c.prefix] = c
        return self.clients[c.prefix]
    
    def setUp(self):
        self.clients = {}
    
    def tearDown(self):
        for c in self.clients.values():
            yield c.flushdb()
    
    def test_meta(self):
        c = self.get_client('yyy')
        self.assertTrue(c.prefix)
        self.assertTrue(c.prefix.startswith('yyy'))
        self.assertTrue(c.client)
        self.assertFalse(c.client.prefix)
        
    def test_delete(self):
        c1 = self.get_client()
        c2 = self.get_client()
        yield c1.set('bla', 'foo')
        yield c2.set('bla', 'foo')
        yield self.async.assertEqual(c1.dbsize(), 1)
        yield self.async.assertEqual(c2.dbsize(), 1)
        yield c1.flushdb()
        yield self.async.assertEqual(c1.dbsize(), 0)
        yield self.async.assertEqual(c2.dbsize(), 1)
        yield c2.flushdb()
        yield self.async.assertEqual(c2.dbsize(), 0)
        
    def test_error(self):
        c = self.get_client()
        self.assertRaises(NotImplementedError, c.execute_command, 'FLUSHDB')
        self.assertRaises(NotImplementedError, c.execute_command, 'FLUSHALL')
        
        
    