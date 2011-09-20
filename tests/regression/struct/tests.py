from stdnet import test, struct


class zsetfunc(object):
    
    def score(self, x):
        return x[0]
    
    def dumps(self, x):
        return x[1]
    
    def loads(self, x):
        return x


class TestStruct(test.TestCase):
    
    def testList(self):
        l = struct.list()
        self.assertEqual(l.size(),0)
        l.push_back(3)
        l.push_back('save')
        self.assertEqual(l.size(),0)
        l.save()
        self.assertEqual(l.size(),2)
        self.assertEqual(list(l),['3','save'])
        
    def testSet(self):
        l = struct.set()
        self.assertEqual(l.size(),0)
        
    def testHash(self):
        l = struct.hash()
        self.assertEqual(l.size(),0)
        
    def testZset(self):
        '''test a very simple zset with integer'''
        l = struct.zset()
        self.assertEqual(l.size(),0)
        l.add(56)
        l.add(-5)
        l.update((5,45,78,-5))
        l.save()
        self.assertEqual(l.size(),5)
        self.assertFalse(l._cache)
        r = list(l)
        self.assertTrue(l._cache)
        self.assertEqual(r,['-5','5','45','56','78'])
        
    def testZsetWithScore(self):
        '''test a very simple zset with integer'''
        z = zsetfunc()
        l = struct.zset(scorefun = z.score, pickler = z)
        self.assertEqual(l.size(),0)
        l.add((39,'luca'))
        l.add((8,'gaia'))
        l.add((40,'jo'))
        l.add((6,'joshua'))
        l.save()
        self.assertEqual(l.size(),4)
        self.assertFalse(l._cache)
        r = list(l)
        self.assertTrue(l._cache)
        self.assertEqual(r,['joshua','gaia','luca','jo'])
    