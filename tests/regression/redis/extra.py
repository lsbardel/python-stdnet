import os

from stdnet import test

from .base import TestCase

class TestZdiffStore(TestCase):
    
    def test_zdiffstore(self):
        self.make_zset('a', {'a1': 1, 'a2': 1, 'a3': 1})
        self.make_zset('b', {'a1': 2, 'a3': 2, 'a4': 2})
        self.make_zset('c', {'a1': 6, 'a3': 5, 'a4': 4})
        n = self.client.zdiffstore('z', ['a', 'b', 'c'])
        r = self.assertEqual(n,1)
        self.assertEquals(
            list(self.client.zrange('z', 0, -1, withscores=True)),
            [(b'a2', 1)])
        
    def test_zdiffstore_withscores(self):
        self.make_zset('a', {'a1': 6, 'a2': 1, 'a3': 2})
        self.make_zset('b', {'a1': 1, 'a3': 1, 'a4': 2})
        self.make_zset('c', {'a1': 3, 'a3': 1, 'a4': 4})
        n = self.client.zdiffstore('z', ['a', 'b', 'c'],
                                   withscores=True)
        r = self.assertEqual(n,2)
        self.assertEquals(
            list(self.client.zrange('z', 0, -1, withscores=True)),
            [(b'a2', 1),(b'a1', 2)])
        
