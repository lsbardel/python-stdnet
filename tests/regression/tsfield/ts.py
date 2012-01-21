from datetime import date, datetime

from stdnet.utils import todate

from examples.tsmodels import TimeSeries, DateTimeSeries
from . import hashts

testdata  = hashts.testdata
testdata2 = hashts.testdata2


class TestDateTimeSeriesTS(hashts.TestHashTimeSeries):
    tag         = 'ts'
    default_run = False
    model       = TimeSeries
        
    def testitems2(self):
        ts = self.filldata(testdata2)
        for k,v in ts.data.items():
            self.assertEqual(v,testdata2[todate(k)])
            
    def testiRange(self):
        ts = self.filldata(testdata2)
        N  = ts.data.size()
        self.assertTrue(N)
        a  = int(N/4)
        b  = 3*a
        r1 = list(ts.data.irange(0,a))
        r2 = list(ts.data.irange(a,b))
        r3 = list(ts.data.irange(b,-1))
        self.assertEqual(r1[-1],r2[0])
        self.assertEqual(r2[-1],r3[0])
        self.assertEqual(r1[0],ts.data.front())
        self.assertEqual(r3[-1],ts.data.back())
        T = len(r1)+len(r2)+len(r3)
        self.assertEqual(T,N+2)
        self.assertEqual(len(r1),a+1)
        self.assertEqual(len(r2),b-a+1)
        self.assertEqual(len(r3),N-b)
        
    def testiRangeTransaction(self):
        ts = self.filldata(testdata2)
        N  = ts.data.size()
        self.assertTrue(N)
        a  = int(N/4)
        b  = 3*a
        with self.model.objects.transaction() as t:
            ts.data.irange(0,a,t)
            ts.data.irange(a,b,t)
            ts.data.irange(b,-1,t)
            ts.data.front(t)
            ts.data.back(t)
        c = lambda x : x if isinstance(x,date) else list(x)
        r1,r2,r3,front,back = [c(r) for r in t.get_result()]
        self.assertEqual(r1[-1],r2[0])
        self.assertEqual(r2[-1],r3[0])
        self.assertEqual(r1[0][0],front)
        self.assertEqual(r3[-1][0],back)
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
        
    def testRangeTransaction(self):
        ts = self.filldata(testdata2)
        N  = ts.data.size()
        a  = int(N/4)
        b  = 3*a
        with self.model.objects.transaction() as t:
            ts.data.irange(0,a,t)
            ts.data.irange(a,b,t)
            ts.data.irange(b,-1,t)
        r1,r2,r3 = [list(r) for r in t.get_result()]
        with self.model.objects.transaction() as t:
            ts.data.range(r2[0][0],r2[-1][0],t)
        r4 = [list(r) for r in t.get_result()][0]
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


class TestDateSeriesTS(TestDateTimeSeriesTS):
    model = DateTimeSeries
    mkdate = date
