from .base import TestCase


class TestStdnetBranchCommand(TestCase):
    
    def test_zdiffstore(self):
        self.make_zset('a', {'a1': 1, 'a2': 1, 'a3': 1})
        self.make_zset('b', {'a1': 2, 'a3': 2, 'a4': 2})
        self.make_zset('c', {'a1': 6, 'a3': 5, 'a4': 4})
        
        self.assert_(self.client.zdiffstore('z', ['a', 'b', 'c']))
        self.assertEquals(
            list(self.client.zrange('z', 0, -1, withscores=True)),
            [(b'a2', 1)])
        
    def test_tsadd(self):
        self.assertEqual(self.client.tslen('a'),0)
        self.client.tsadd('a', 3, 'a', 1, 'b', 7, 'a')
        ts = list(self.client.tsrange('a', 0, -1, withtimes = True))
        self.assertEqual(ts, [(1, b'b'),(3, b'a'),(7, b'a')])
        
    def test_tsrank(self):
        self.client.tsadd('a', 3, 'a', 1, 'b', 7, 'a')
        self.assertEqual(self.client.tsrank('a',1),0)
        self.assertEqual(self.client.tsrank('a',3),1)
        self.assertEqual(self.client.tsrank('a',7),2)
        self.assertEqual(self.client.tsrank('a',8),None)
        self.assertEqual(self.client.tsrank('a',-1),None)
        self.assertEqual(self.client.tsrank('a',5),None)
        self.assertEqual(self.client.tsrank('b',5),None)
        self.assertEqual(self.client.zrank('b',5),None)
        
    def test_tsrange_novalues(self):
        self.client.tsadd('a', 3, 'a', 1, 'b', 7, 'a')
        times = tuple(self.client.tsrange('a',novalues=True))
        self.assertEqual(times,(1,3,7))
    