from datetime import date

from stdnet import test, orm
from stdnet.utils import encoders, zip
from stdnet.utils.populate import populate


dates = list(set(populate('date',100,start=date(2009,6,1),end=date(2010,6,6))))
values = populate('float',len(dates),start=0,end=1000)


class TestSet(test.TestCase):
    
    def testMeta(self):
        l = orm.Set()
        self.assertEqual(l.id,None)
        self.assertEqual(l.instance,None)
        self.assertEqual(l.session,None)
        self.assertEqual(l._meta.name,'set')
        self.assertEqual(l._meta.model._model_type,'structure')
        
    def testSimpleUpdate(self):
        # Typical usage
        session = self.session()
        s = session.add(orm.Set())
        self.assertEqual(s.session,session)
        self.assertEqual(s.instance,None)
        self.assertEqual(s.id,None)
        s.add(8)
        s.update((1,2,3,4,5,5))
        session.commit()
        self.assertTrue(s.id)
        self.assertEqual(s.size(),6)
        
    def testUpdateDelete(self):
        session = self.session()
        with session.begin():
            s = session.add(orm.Set())
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
        

class TestZset(test.TestCase):
    result =  [(0.0022,'pluto'),
               (0.06,'mercury'),
               (0.11,'mars'),
               (0.82,'venus'),
               (1,'earth'),
               (14.6,'uranus'),
               (17.2,'neptune'),
               (95.2,'saturn'),
               (317.8,'juppiter')]
        
    def planets(self):
        session = self.session()
        l = session.add(orm.Zset())
        l.add(1,'earth')
        l.add(0.06,'mercury')
        l.add(317.8,'juppiter')
        l.update(((95.2,'saturn'),\
                  (0.82,'venus'),\
                  (14.6,'uranus'),\
                  (0.11,'mars'),
                  (17.2,'neptune'),
                  (0.0022,'pluto')))
        self.assertEqual(l.size(),0)
        self.assertEqual(len(l.cache.toadd),9)
        session.commit()
        self.assertEqual(l.size(),9)
        return l
            
    def testMeta(self):
        session = self.session()
        l = session.add(orm.Zset())
        self.assertEqual(l._meta.name,'zset')
        self.assertEqual(l._meta.model._model_type,'structure')
        self.assertEqual(l.size(),0)
        self.assertFalse(l.cache.cache)
        self.assertFalse(l.cache.toadd)
        self.assertFalse(l.cache.toremove)

    def testZsetState(self):
        '''test a very simple zset with integer'''
        session = self.session()
        z = session.add(orm.Zset())
        self.assertFalse(z.state().persistent)
        self.assertTrue(z in session)
        self.assertEqual(z.session,session)
        session.delete(z)
        self.assertFalse(z in session)
        self.assertEqual(z.session, None)
        
    def testiRange(self):
        l = self.planets()
        r = l.irange()
        
    def testIter(self):
        '''test a very simple zset with integer'''
        l = self.planets()
        r = list(l)
        v = [t[1] for t in self.result]
        self.assertEqual(r,v)
                
    def testItems(self):
        '''test a very simple zset with integer'''
        l = self.planets()
        r = list(l.items())
        self.assertEqual(r,self.result)
        
    def testGet(self):
        l = self.planets()
        self.assertEqual(l[1],'earth')
        self.assertEqual(l.get(0.11),'mars')


class TestList(test.TestCase):

    def testMeta(self):
        session = self.session()
        l = session.add(orm.List())
        self.assertEqual(l._meta.name,'list')
        self.assertEqual(l._meta.model._model_type,'structure')
        self.assertEqual(l.size(),0)
        l.push_back(3)
        l.push_back(5.6)
        l.push_back('save')
        l.push_back({'test': 1})
        self.assertEqual(l.size(),0)
        session.commit()
        self.assertEqual(l.size(),4)
        self.assertEqual(list(l),[3,5.6,'save',"{'test': 1}"])
        
    def testJsonList(self):
        session = self.session()
        l = session.add(orm.List(value_pickler = encoders.Json()))
        self.assertEqual(l.size(),0)
        l.push_back(3)
        l.push_back(5.6)
        l.push_back('save')
        l.push_back({'test': 1})
        self.assertEqual(l.size(),0)
        session.commit()
        self.assertEqual(l.size(),4)
        self.assertEqual(list(l),[3,5.6,'save',{'test': 1}])
    

class TestHash(test.TestCase):
    
    def testMeta(self):
        session = self.session()
        h = session.add(orm.HashTable())
        self.assertEqual(h._meta.name,'hashtable')
        self.assertEqual(h._meta.model._model_type,'structure')
        self.assertEqual(h.size(),0)
        with session.begin() as t:
            h['bla'] = 'foo'
            h['pluto'] = 3
        self.assertEqual(h.size(),2)
        
    def testPop(self):
        session = self.session()
        with session.begin():
            d = session.add(orm.HashTable())
            d['foo'] = 'ciao'
        self.assertEqual(d.size(),1)
        self.assertEqual(d['foo'],'ciao')
        self.assertRaises(KeyError, lambda : d.pop('bla'))
        self.assertEqual(d.pop('bla',56),56)
        self.assertRaises(TypeError, lambda : d.pop('bla',1,2))
        self.assertEqual(d.pop('foo'),'ciao')
        self.assertEqual(len(d),0)
        
    def testGet(self):
        session = self.session()
        with session.begin():
            h = session.add(orm.HashTable())
            h['bla'] = 'foo'
            h['bee'] = 3
        
        self.assertEqual(h['bla'],'foo')
        self.assertEqual(h.get('bee'),3)
        self.assertEqual(h.get('ggg'),None)
        self.assertEqual(h.get('ggg',1),1)
        self.assertRaises(KeyError, lambda : h['gggggg'])
        

class TestTS(test.TestCase):
    
    def testMeta(self):
        session = self.session()
        ts = session.add(orm.TS())
        self.assertEqual(ts._meta.name,'ts')
        self.assertEqual(ts._meta.model._model_type,'structure')
        self.assertEqual(ts.size(),0)
        self.assertFalse(ts.cache.cache)
        self.assertFalse(ts.cache.toadd)
        self.assertFalse(ts.cache.toremove)
        
    def testEmpty(self):
        session = self.session()
        ts = session.add(orm.TS())
        self.assertFalse(ts.id)
        self.assertEqual(ts.size(),0)
        self.assertEqual(ts.front(),None)
        self.assertEqual(ts.back(),None)
        self.assertEqual(ts.size(),0)
        
    def testData(self):
        session = self.session()
        ts = session.add(orm.TS())
        ts.update(zip(dates,values))
        self.assertTrue(ts.cache.toadd)
        session.commit()
        self.assertEqual(ts.size(),len(dates))
        front = ts.front()
        back = ts.back()
        self.assertTrue(back[0]>front[0])
        range = list(ts.range(date(2009,10,1),date(2010,5,1)))
        self.assertTrue(range)
        for time,val in range:
            self.assertTrue(time>=front[0])
            self.assertTrue(time<=back[0])
            
    def testGet(self):
        session = self.session()
        with session.begin():
            ts = session.add(orm.TS())
            ts.update(zip(dates,values))
        dt1 = dates[0]
        val1 = ts[dt1]
        self.assertTrue(val1)
        self.assertEqual(ts.get(dt1),val1)
        self.assertEqual(ts.get(date(1990,1,1)),None)
        self.assertEqual(ts.get(date(1990,1,1),1),1)
        self.assertRaises(KeyError, lambda : ts[date(1990,1,1)])
    