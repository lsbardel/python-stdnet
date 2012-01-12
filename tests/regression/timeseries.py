from datetime import date, timedelta
from stdnet import test, struct
from stdnet.apps.timeseries2.models import TimeSeriesField 


class TestTimeSeries(test.TestCase):
    
    def testEmpty(self):
        ts = struct.ts2()
        self.assertEqual(ts.size(),0)
        self.assertEqual(ts.numfields(),0)
        self.assertEqual(ts.fields(),set())
        
    def testAdd(self):
        ts = struct.ts2()
        ts.add(date.today(),'pv',56)
        self.assertEqual(ts.size(),0)
        self.assertTrue(ts.cache.fields)
        ts.add(date.today()-timedelta(days=2),'pv',56.8)
        self.assertTrue(len(ts.cache.fields['pv']),2)
        ts.commit()