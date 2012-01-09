from datetime import date

from stdnet import test, orm
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


#class TestStruct(test.TestCase):
class TestStruct(object):
    
    def testList(self):
        session = self.session()
        l = session.add(orm.List())
        self.assertEqual(l._meta.name,'list')
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
        
    def testZset(self):
        '''test a very simple zset with integer'''
        l = orm.Zset()
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
        l = orm.Zset(scorefun = z.score, pickler = z)
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
        

class TestSetStructure(test.TestCase):
    
    def testMeta(self):
        l = orm.Set()
        self.assertEqual(l.id,None)
        self.assertEqual(l.instance,None)
        self.assertEqual(l.session,None)
        self.assertEqual(l._meta.name,'set')
        
    def testSimpleUpdate(self):
        # Typical usage
        session = self.session()
        s = session.add(orm.Set())
        self.assertEqual(s.session,session)
        self.assertEqual(s.instance,None)
        self.assertEqual(s.id,None)
        with session.begin():
            s.add(8)
            s.update((1,2,3,4,5,5))
        self.assertTrue(s.id)
        self.assertEqual(s.size(),6)
        
    def testUpdateDelete(self):
        session = self.session()
        s = session.add(orm.Set())
        with session.begin():
            s.update((1,2,3,4,5,5))
            s.discard(2)
            s.discard(67)
            s.remove(4)
            s.remove(46)
            s.difference_update((1,56,89))
        self.assertEqual(s.size(),2)
        with session.begin():
            s.difference_update((3,5,6,7))
        self.assertEqual(s.size(),0)    
        

class TestHash(test.TestCase):
    
    def testSimple(self):
        l = orm.HashTable()
        self.assertRaises(ValueError, l.size)
        session = self.session()
        h = session.add(orm.HashTable())
        with session.begin():
            h['bla'] = 'foo'
            h['pluto'] = 3
        self.assertEqual(h.size(),2)
        #d = dict(l)
        #self.assertEqual(d,{'bla':b'foo','pluto':b'3'})
        
    def __testPop(self):
        d = struct.dict()
        d['foo'] = 'ciao'
        d.save()
        self.assertEqual(len(d),1)
        self.assertEqual(d['foo'],'ciao')
        self.assertRaises(KeyError, lambda : d.pop('bla'))
        self.assertEqual(d.pop('bla',56),56)
        self.assertRaises(TypeError, lambda : d.pop('bla',1,2))
        self.assertEqual(d.pop('foo'),'ciao')
        self.assertEqual(len(d),0)
        

#class TestTimeserie(test.TestCase):
class TestTimeserie(object):
    
    def testEmpty(self):
        ts = orm.TS()
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
        ts = orm.TS()
        ts.update(zip(dates,values))
        ts.save()
        self.assertEqual(ts.size(),len(dates))
        front = ts.front()
        back = ts.back()
        self.assertTrue(back>front)
        range = list(ts.range(date(2009,10,1),date(2010,5,1)))
        self.assertTrue(range)
        
    