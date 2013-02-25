import os
from datetime import date

from stdnet import odm
from stdnet.utils import test, encoders, zip
from stdnet.utils.populate import populate

from .base import StructMixin

dates = list(set(populate('date',100,start=date(2009,6,1),end=date(2010,6,6))))
values = populate('float',len(dates),start=0,end=1000)


class TestZset(StructMixin, test.CleanTestCase):
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
    
    def createOne(self, session):
        l = session.add(odm.Zset())
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
        session.begin()
        l = self.createOne(session)
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
        z = session.add(odm.Zset())
        self.assertTrue(z.state().persistent)
        self.assertTrue(z in session)
        self.assertEqual(z.session, session)
        session.delete(z)
        self.assertFalse(z in session)
        self.assertTrue(z.session)
        
    def test_irange(self):
        l = self.planets()
        # Get the whole range
        r = list(l.irange())
        self.assertEqual(r,self.result)
        
    def test_range(self):
        l = self.planets()
        r = list(l.range(0.5,20))
        self.assertTrue(r)
        k1 = 0.5
        for k,v in r:
            self.assertTrue(k>=k1)
            self.assertTrue(k<=20)
            k1 = k
        
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
        self.assertEqual(r, self.result)

