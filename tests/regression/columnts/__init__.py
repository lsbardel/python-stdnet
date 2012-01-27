import os
from datetime import date, datetime, timedelta

from stdnet import test
from stdnet.apps.columnts import ColumnTS, TimeSeriesField
from stdnet.apps.columnts.redis import script_path
from stdnet.lib import redis


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
        
    def testAddSimple(self):
        session = self.session()
        ts = session.add(ColumnTS(id = 'goog'))
        ts.add(date.today(),'pv',56)
        self.assertEqual(ts.size(),0)
        self.assertTrue(ts.cache.fields)
        ts.add(date.today()-timedelta(days=2),'pv',56.8)
        self.assertTrue(len(ts.cache.fields['pv']),2)
        session.commit()
        self.assertEqual(ts.fields(),('pv',))
        self.assertEqual(ts.numfields(),1)
        self.assertEqual(ts.size(),2)
        
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