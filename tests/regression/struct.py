from datetime import date

from stdnet import test, struct, transaction
from stdnet.utils import encoders, zip
from stdnet.utils.populate import populate


dates = list(set(populate('date',100,start=date(2009,6,1),end=date(2010,6,6))))
values = populate('float',len(dates),start=0,end=1000)


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
        self.assertEqual(list(l),[b'3',b'save'])
        
    def testJsonList(self):
        l = struct.list(pickler = encoders.Json())
        self.assertEqual(l.size(),0)
        l.push_back(3)
        l.push_back('save')
        self.assertEqual(l.size(),0)
        l.save()
        self.assertEqual(l.size(),2)
        self.assertEqual(list(l),[3,'save'])
        
    def testSet(self):
        l = struct.set()
        self.assertEqual(l.size(),0)
        
    def testHash(self):
        l = struct.hash()
        self.assertEqual(l.size(),0)
        l['bla'] = 'foo'
        l['pluto'] = 3
        l.save()
        self.assertEqual(l.size(),2)
        d = dict(l)
        self.assertEqual(d,{'bla':b'foo','pluto':b'3'})
        
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
        self.assertEqual(r,[b'-5',b'5',b'45',b'56',b'78'])
        
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
        self.assertEqual(r,[b'joshua',b'gaia',b'luca',b'jo'])
        
        
class TestTimeserie(test.TestCase):
    
    def testEmpty(self):
        ts = struct.ts()
        self.assertEqual(ts.front(),None)
        self.assertEqual(ts.back(),None)
        # lets try with a transaction
        with transaction(ts) as t:
            ts.front(t)
            ts.back(t)
        for r in t.get_result():
            self.assertEqual(r,None)
        self.assertEqual(ts.size(),0)
        
    def testData(self):
        ts = struct.ts()
        ts.update(zip(dates,values))
        ts.save()
        self.assertEqual(ts.size(),len(dates))
        front = ts.front()
        back = ts.back()
        self.assertTrue(back>front)
        range = list(ts.range(date(2009,10,1),date(2010,5,1)))
        self.assertTrue(range)
        
    