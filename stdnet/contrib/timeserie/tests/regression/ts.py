from datetime import date, datetime

from stdnet.contrib.timeserie.tests.models import TimeSeries
from stdnet.contrib.timeserie.tests.regression import hashtimeseries

testdata  = hashtimeseries.testdata
testdata2 = hashtimeseries.testdata2


class TestDateTimeSeriesTS(hashtimeseries.TestHashTimeSeries):
    tag         = 'ts'
    default_run = False
    model       = TimeSeries
        
    def testitems2(self):
        ts = self.filldata(testdata2)
        for k,v in ts.data.items():
            self.assertEqual(v,testdata2[k.date()])
            
    def testiRange(self):
        ts = self.filldata(testdata2)
        N  = ts.data.size()
        a  = int(N/4)
        b  = 3*a
        r1 = list(ts.data.irange(0,a))
        r2 = list(ts.data.irange(a,b))
        r3 = list(ts.data.irange(b,-1))
        self.assertEqual(r1[-1],r2[0])
        self.assertEqual(r2[-1],r3[0])
        self.assertEqual(r1[0][0],ts.data.front())
        self.assertEqual(r3[-1][0],ts.data.back())
        T = len(r1)+len(r2)+len(r3)
        self.assertEqual(T,N+2)
        self.assertEqual(len(r1),a+1)
        self.assertEqual(len(r2),b-a+1)
        self.assertEqual(len(r3),N-b)
        
    def testRange(self):
        ts = self.filldata(testdata2)
        N  = ts.data.size()
        a  = int(N/4)
        b  = 3*a
        r1 = list(ts.data.irange(0,a))
        r2 = list(ts.data.irange(a,b))
        r3 = list(ts.data.irange(b,-1))
        r4 = list(ts.data.range(r2[0][0],r2[-1][0]))
        self.assertEqual(r4[0],r2[0])
        self.assertEqual(r4[-1],r2[-1])
        
    def testCount(self):
        ts = self.filldata(testdata2)
        N  = ts.data.size()
        a  = int(N/4)
        b  = 3*a
        r1 = list(ts.data.irange(0,a))
        r2 = list(ts.data.irange(a,b))
        r3 = list(ts.data.irange(b,-1))
        self.assertEqual(ts.data.count(r2[0][0],r2[-1][0]),b-a+1)
