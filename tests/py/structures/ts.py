import os
from datetime import date

from stdnet import odm, InvalidTransaction
from stdnet.utils import test, encoders, zip
from stdnet.utils.populate import populate

dates = list(set(populate('date',100,start=date(2009,6,1),end=date(2010,6,6))))
values = populate('float',len(dates),start=0,end=1000)

from .base import StructMixin

class TestTS(StructMixin, test.CleanTestCase):
    structure = odm.TS
    name = 'ts'
    
    def createOne(self, session):
        ts = session.add(odm.TS())
        ts.update(zip(dates,values))
        return ts
        
    def testMeta2(self):
        ts = self.testMeta()
        self.assertFalse(ts.cache.cache)
        self.assertTrue(ts.cache.toadd)
        self.assertFalse(ts.cache.toremove)
        
    def testEmpty2(self):
        session = self.session()
        ts = session.add(odm.TS())
        self.assertTrue(ts.id)
        self.assertEqual(ts.size(),0)
        self.assertEqual(ts.front(),None)
        self.assertEqual(ts.back(),None)
        self.assertEqual(ts.size(),0)
        
    def testData(self):
        session = self.session()
        session.begin()
        ts = self.createOne(session)
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
            ts = session.add(odm.TS())
            ts.update(zip(dates,values))
        dt1 = dates[0]
        val1 = ts[dt1]
        self.assertTrue(val1)
        self.assertEqual(ts.get(dt1),val1)
        self.assertEqual(ts.get(date(1990,1,1)),None)
        self.assertEqual(ts.get(date(1990,1,1),1),1)
        self.assertRaises(KeyError, lambda : ts[date(1990,1,1)])
        
    def testPop(self):
        session = self.session()
        with session.begin():
            ts = session.add(odm.TS())
            ts.update(zip(dates,values))
        dt = dates[5]
        self.assertTrue(dt in ts)
        v = ts.pop(dt)
        self.assertTrue(v)
        self.assertFalse(dt in ts)
        self.assertRaises(KeyError, ts.pop, dt)
        self.assertEqual(ts.pop(dt,'bla'), 'bla')
        
    def test_rank_ipop(self):
        session = self.session()
        with session.begin() as t:
            ts = t.add(odm.TS())
            ts.update(zip(dates, values))
        yield t.on_result
        dt = dates[5]
        value = ts.get(dt)
        r = ts.rank(dt)
        all_dates = list((d.date() for d in ts.itimes()))
        self.assertEqual(all_dates[r], dt)
        value2 = ts.ipop(r)
        self.assertEqual(value, value2)
        self.assertFalse(dt in ts)
        
    def test_pop_range(self):
        session = self.session()
        with session.begin():
            ts = session.add(odm.TS())
            ts.update(zip(dates,values))
        all_dates = list((d.date() for d in ts.itimes()))
        range = list(ts.range(all_dates[5],all_dates[15]))
        self.assertTrue(range)
        range2 = list(ts.pop_range(all_dates[5],all_dates[15]))
        self.assertEqual(range, range2)
        for dt,_ in range:
            self.assertFalse(dt in ts) 
