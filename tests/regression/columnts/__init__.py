import os
from datetime import date, datetime, timedelta

from stdnet import test
from stdnet.apps.columnts import ColumnTS, TimeSeriesField
from stdnet.apps.columnts.redis import script_path
from stdnet.lib import redis

from examples.data import tsdata

this_path = os.path.split(os.path.abspath(__file__))[0]

class timeseries_test1(redis.RedisScript):
    script = (redis.read_lua_file('columnts.lua',script_path),
              redis.read_lua_file('test1.lua',this_path))
    

class TestLuaClass(test.TestCase):
    
    def test1(self):
        session = self.session()
        ts = session.add(ColumnTS(id = 'bla'))
        c = self.backend.client
        r = c.script_call('timeseries_test1',ts.dbid())
        self.assertEqual(r,b'OK')

    
class TestTimeSeries(test.TestCase):
    
    def makeGoogle(self):
        session = self.session()
        with session.begin():
            ts = session.add(ColumnTS(id = 'goog'))
            ts.add(date(2012,1,23),{'open':586, 'high':588.66,
                                    'low':583.16, 'close':585.52})
            ts.update(((date(2012,1,20),{'open':590.53, 'high':591,
                                        'low':581.7, 'close':585.99}),
                       (date(2012,1,19),{'open':640.99, 'high':640.99,
                                        'low':631.46, 'close':639.57})))
            self.assertTrue(len(ts.cache.fields['open']),2)
            self.assertTrue(len(ts.cache.fields),4)
        return ts
            
    def testEmpty(self):
        session = self.session()
        ts = session.add(ColumnTS(id = 'goog'))
        self.assertEqual(ts.size(),0)
        self.assertEqual(ts.numfields(),0)
        self.assertEqual(ts.fields(),())
        
    def testEmpty2(self):
        session = self.session()
        with session.begin():
            ts = session.add(ColumnTS())
        self.assertEqual(ts.size(),0)
        
    def testAddSimple(self):
        session = self.session()
        ts = session.add(ColumnTS(id = 'goog'))
        ts.add(date.today(),'pv',56)
        self.assertEqual(ts.size(),0)
        self.assertTrue(ts.cache.fields)
        ts.add(date.today()-timedelta(days=2),'pv',56.8)
        self.assertTrue(len(ts.cache.fields['pv']),2)
        # commit
        session.commit()
        self.assertEqual(ts.fields(),('pv',))
        self.assertEqual(ts.numfields(),1)
        self.assertEqual(ts.size(),2)
        #
        # Check that a string is available at the field key
        bts = ts.backend_structure()
        keys = tuple(bts.allkeys())
        self.assertEqual(len(keys),3)
        self.assertTrue(bts.id in keys)
        self.assertTrue(bts.fieldsid in keys)
        self.assertTrue(bts.fieldid('pv') in keys)
        raw_data = bts.field('pv')
        self.assertTrue(raw_data)
        self.assertEqual(len(raw_data),18)
        #
        data = ts.range()
        self.assertEqual(len(data),2)
        dt,fields = data
        self.assertEqual(len(dt),2)
        self.assertTrue('pv' in fields)
        self.assertEqual(fields['pv'],[56.8,56])
        
    def testGoogleDrop(self):
        ts = self.makeGoogle()
        self.assertEqual(ts.fields(),('close','high','low','open'))
        self.assertEqual(ts.numfields(),4)
        self.assertEqual(ts.size(),3)
        
    def testRange(self):
        ts = self.makeGoogle()
        data = ts.range()
        self.assertEqual(len(data),2)
        dt,fields = data
        self.assertEqual(len(fields),4)
        high = list(zip(dt,fields['high']))
        self.assertEqual(high[0],(datetime(2012,1,19),640.99))
        self.assertEqual(high[1],(datetime(2012,1,20),591))
        self.assertEqual(high[2],(datetime(2012,1,23),588.66))
        

class TestOperations(test.TestCase):
    
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data1 = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        cls.data2 = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        cls.data3 = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        
    def testSimpleStats(self):
        session = self.session()
        with session.begin():
            ts1 = session.add(ColumnTS())
            ts1.update(self.data1.values)
        dt,fields = ts1.range()
        self.assertEqual(len(fields),6)
        result = ts1.stats(0,-1)
        self.assertTrue(result)
        self.assertEqual(result['start'],dt[0])
        self.assertEqual(result['stop'],dt[-1])
        self.assertEqual(result['len'],len(dt))
        stats = result['stats']
        for field in ('a','b','c','d','f','g'):
            self.assertTrue(field in stats)
            stat_field = stats[field]
            data = self.data1.fields[field]
            self.assertAlmostEqual(stat_field[0], min(data))
            self.assertAlmostEqual(stat_field[1], max(data))
            
        
    def test_merge2series(self):
        session = self.session()
        with session.begin():
            ts1 = session.add(ColumnTS())
            ts2 = session.add(ColumnTS())
            ts1.update(self.data1.values)
            ts2.update(self.data2.values)
        self.assertEqual(ts1.size(),len(self.data1.unique_dates))
        self.assertEqual(ts1.numfields(),6)
        self.assertEqual(ts2.size(),len(self.data2.unique_dates))
        self.assertEqual(ts2.numfields(),6)
        ts3 = ColumnTS(id = 'merged')
        # merge ts1 with weight -1  and ts2 with weight 2
        ts3.merge((ts1,-1),(ts2,2))
        session.commit()
        self.assertTrue(ts3.size())
        self.assertEqual(ts3.numfields(),6)
        