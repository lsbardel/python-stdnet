from datetime import date

from stdnet import test, orm
from stdnet.utils import encoders, zip
from stdnet.utils.populate import populate


dates = list(set(populate('date',100,start=date(2009,6,1),end=date(2010,6,6))))
values = populate('float',len(dates),start=0,end=1000)


class StructMixin(object):
    name = None
    
    def createOne(self):
        raise NotImplementedError()
    
    def testMeta(self):
        session = self.session()
        l = self.createOne()
        self.assertTrue(l.id)
        self.assertEqual(l.instance, None)
        self.assertEqual(l.session, None)
        self.assertEqual(l._meta.name, self.name)
        self.assertEqual(l._meta.model._model_type,'structure')
        self.assertFalse(l.state().persistent)
        # add to a session
        l = session.add(l)
        # If no id available, adding to a session will automatically create one
        self.assertTrue(l.id)
        self.assertEqual(l.session, session)
        self.assertEqual(l.size(), 0)
        return l
        
    def testDelete(self):
        session = self.session()
        with session.begin():
            s = session.add(self.createOne())
        self.assertTrue(s.session)
        self.assertTrue(s.state().persistent)
        self.assertFalse(s in session)
        self.assertTrue(s.size())
        s.delete()
        self.assertEqual(s.size(),0)


class TestSet(StructMixin,test.TestCase):
    name = 'set'    
    
    def createOne(self):
        s = orm.Set()
        s.update((1,2,3,4,5,5))
        return s
            
    def testSimpleUpdate(self):
        # Typical usage
        session = self.session()
        s = session.add(orm.Set())
        self.assertEqual(s.session, session)
        self.assertEqual(s.instance, None)
        s.add(8)
        s.update((1,2,3,4,5,5))
        self.assertFalse(s.state().persistent)
        session.commit()
        self.assertTrue(s.id)
        self.assertEqual(s.size(),6)
        self.assertTrue(s.state().persistent)
        
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
        

class TestZset(StructMixin,test.TestCase):
    name = 'zset'
    result =  [(0.0022,'pluto'),
               (0.06,'mercury'),
               (0.11,'mars'),
               (0.82,'venus'),
               (1,'earth'),
               (14.6,'uranus'),
               (17.2,'neptune'),
               (95.2,'saturn'),
               (317.8,'juppiter')]
    
    def createOne(self):
        l = orm.Zset()
        l.add(1,'earth')
        l.add(0.06,'mercury')
        l.add(317.8,'juppiter')
        l.update(((95.2,'saturn'),\
                  (0.82,'venus'),\
                  (14.6,'uranus'),\
                  (0.11,'mars'),
                  (17.2,'neptune'),
                  (0.0022,'pluto')))
        self.assertEqual(len(l.cache.toadd),9)
        return l
        
    def planets(self):
        session = self.session()
        l = session.add(self.createOne())
        self.assertEqual(l.size(),0)
        session.commit()
        self.assertTrue(l.state().persistent)
        self.assertEqual(l.size(),9)
        return l
            
    def testMeta2(self):
        l = self.testMeta()
        self.assertFalse(l.cache.cache)
        self.assertTrue(l.cache.toadd)
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


class TestList(StructMixin, test.TestCase):
    name = 'list'
    
    def createOne(self):
        l = orm.List()
        l.push_back(3)
        l.push_back(5.6)
        return l
    
    def testMeta2(self):
        l = self.testMeta()
        l.push_back('save')
        l.push_back({'test': 1})
        l.save()
        sm = l.session.model(l._meta)
        self.assertEqual(len(sm),0)
        self.assertTrue(l in sm.loaded)
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
        l.push_back({'test': 2})
        self.assertEqual(l.size(),0)
        session.commit()
        self.assertEqual(l.size(),5)
        self.assertEqual(list(l),[3,5.6,'save',{'test': 1},{'test': 2}])
    

class TestHash(StructMixin, test.TestCase):
    name = 'hashtable'
    
    def createOne(self):
        h = orm.HashTable()
        h['bla'] = 'foo'
        h['pluto'] = 3
        return h
    
    def testMeta2(self):
        h = self.testMeta()
        h.save()
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
        

class TestTS(StructMixin, test.TestCase):
    name = 'ts'
    
    def createOne(self):
        ts = orm.TS()
        ts.update(zip(dates,values))
        return ts
        
    def testMeta2(self):
        ts = self.testMeta()
        self.assertFalse(ts.cache.cache)
        self.assertTrue(ts.cache.toadd)
        self.assertFalse(ts.cache.toremove)
        
    def testEmpty(self):
        session = self.session()
        ts = session.add(orm.TS())
        self.assertTrue(ts.id)
        self.assertEqual(ts.size(),0)
        self.assertEqual(ts.front(),None)
        self.assertEqual(ts.back(),None)
        self.assertEqual(ts.size(),0)
        
    def testData(self):
        session = self.session()
        ts = session.add(self.createOne())
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
    