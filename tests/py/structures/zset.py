import os
from datetime import date

from stdnet import odm
from stdnet.utils import test, encoders, zip
from stdnet.utils.populate import populate

from .base import StructMixin

dates = list(set(populate('date',100,start=date(2009,6,1),end=date(2010,6,6))))
values = populate('float',len(dates),start=0,end=1000)


class TestZset(StructMixin, test.TestCase):
    structure = odm.Zset
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
    
    def create_one(self):
        l = odm.Zset()
        l.add(1,'earth')
        l.add(0.06,'mercury')
        l.add(317.8,'juppiter')
        l.update(((95.2,'saturn'),\
                  (0.82,'venus'),\
                  (14.6,'uranus'),\
                  (0.11,'mars'),
                  (17.2,'neptune'),
                  (0.0022,'pluto')))
        self.assertEqual(len(l.cache.toadd), 9)
        self.assertFalse(l.cache.cache)
        self.assertTrue(l.cache.toadd)
        self.assertFalse(l.cache.toremove)
        return l
        
    def planets(self):
        with self.session().begin() as t:
            l = t.add(self.create_one())
            size = yield l.size()
            self.assertEqual(size, 0)
        yield t.on_result
        self.assertTrue(l.state().persistent)
        size = yield l.size()
        self.assertEqual(size, 9)
        yield l
        
    def testZsetState(self):
        '''test a very simple zset with integer'''
        session = self.session()
        z = session.add(odm.Zset())
        self.assertTrue(z.state().persistent)
        self.assertTrue(z in session)
        self.assertEqual(z.session, session)
        session.delete(z)
        self.assertFalse(z in session)
        self.assertTrue(z.session)
        
    def test_irange(self):
        l = yield self.planets()
        # Get the whole range without the scores
        r = yield l.irange(withscores=False)
        self.assertEqual(r, [v[1] for v in self.result])
        
    def test_irange_withscores(self):
        l = yield self.planets()
        # Get the whole range
        r = yield l.irange()
        self.assertEqual(r, self.result)
        
    def test_range(self):
        l = yield self.planets()
        r = yield l.range(0.5, 20, withscores=False)
        self.assertEqual(r, ['venus', 'earth', 'uranus', 'neptune'])
            
    def test_range_withscores(self):
        l = yield self.planets()
        r = yield l.range(0.5,20)
        self.assertTrue(r)
        k1 = 0.5
        for k, v in r:
            self.assertTrue(k>=k1)
            self.assertTrue(k<=20)
            k1 = k
        
    def test_iter(self):
        '''test a very simple zset with integer'''
        l = yield self.planets()
        r = list(l)
        v = [t[1] for t in self.result]
        self.assertEqual(r,v)
                
    def testItems(self):
        '''test a very simple zset with integer'''
        l = yield self.planets()
        r = list(l.items())
        self.assertEqual(r, self.result)

