from datetime import date, datetime
from random import uniform

from stdnet.test import TestCase
from stdnet.contrib.timeserie.utils import dategenerator, default_parse_interval
from stdnet.utils import populate, todate, zip

from stdnet.contrib.timeserie.tests.models import HashTimeSeries, DateHashTimeSeries


NUM_DATES = 300

dates     = populate('date',NUM_DATES)
dates2    = populate('date',NUM_DATES,start=date(2009,1,1),end=date(2010,1,1))
values    = populate('float',NUM_DATES, start = 10, end = 400)
alldata   = list(zip(dates,values))
alldata2  = list(zip(dates2,values))
testdata  = dict(alldata)
testdata2 = dict(alldata2)


class TestHashTimeSeries(TestCase):
    model   = HashTimeSeries
    mkdate  = datetime
    
    def setUp(self):
        self.orm.register(self.model)
        #self.orm.clearall()
        self.model(ticker = 'GOOG').save()
        
    def unregister(self):
        self.orm.unregister(self.model)
        
    def get(self, ticker = 'GOOG'):
        return self.model.objects.get(ticker = ticker)
        
    def filldata(self, data = None):
        data = data or testdata
        d = self.get()
        d.data.update(data)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(data))
        return self.get()

    def interval(self, a, b, targets, C, D):
        ts = self.get()
        intervals = ts.intervals(a,b)
        self.assertEqual(len(intervals),len(targets))
        for interval,target in zip(intervals,targets):
            x = interval[0]
            y = interval[1]
            self.assertEqual(x,target[0])
            self.assertEqual(y,target[1])
            for dt in dategenerator(x,y):
                ts.data.add(dt,uniform(0,1))
        ts.save()
        self.assertEqual(ts.start,C)
        self.assertEqual(ts.end,D)
        
    def testFrontBack(self):
        ts = self.get()
        self.assertEqual(ts.start,None)
        self.assertEqual(ts.end,None)
        mkdate = self.mkdate
        ts.data.update(testdata2)
        ts.save()
        start = ts.start
        end   = ts.end
        p = start
        for d in ts.dates():
            self.assertTrue(d>=p)
            p = d
        self.assertEqual(d,end)
        
    def testkeys(self):
        ts = self.filldata()
        keyp = None
        data = testdata.copy()
        for key in ts.dates():
            if keyp:
                self.assertTrue(key,keyp)
            keyp = key
            data.pop(todate(key))
        self.assertEqual(len(data),0)
        
    def testitems(self):
        ts = self.filldata()
        keyp = None
        data = testdata.copy()
        for key,value in ts.items():
            if keyp:
                self.assertTrue(key,keyp)
            keyp = key
            self.assertEqual(data.pop(todate(key)),value)
        self.assertEqual(len(data),0)
    
    def testUpdate(self):
        ts = self.get()
        dt1 = self.mkdate(2010,5,6)
        dt2 = self.mkdate(2010,6,6)
        ts.data[dt1] = 56
        ts.data[dt2] = 88
        ts.save()
        self.assertEqual(ts.data[dt1],56)
        self.assertEqual(ts.data[dt2],88)
        ts.data[dt1] = "ciao"
        ts.save()
        self.assertEqual(ts.data[dt1],"ciao")
        
    def testInterval(self):
        '''Test interval handling'''
        mkdate = self.mkdate
        ts = self.get()
        self.assertEqual(ts.start,None)
        self.assertEqual(ts.end,None)
        #
        #
        A1   = mkdate(2010,5,10)
        B1   = mkdate(2010,5,12)
        self.interval(A1,B1,[[A1,B1]],A1,B1)
        #
        #  original ->      A1      B1
        #  request  -> A2      B2
        #  interval -> A2  A1-
        #  range    -> A2          B1
        A2   = mkdate(2010,5,6)
        B2   = mkdate(2010,5,11)
        self.interval(A2,B2,[[A2,default_parse_interval(A1,-1)]],A2,B1)
        #
        #  original ->      A2      B1
        #  request  -> A3                B3
        #  interval -> A3  A2-       B1+ B3
        #  range    -> A3                B3
        A3   = mkdate(2010,5,4)
        B3   = mkdate(2010,5,14)
        self.interval(A3,B3,[[A3,default_parse_interval(A2,-1)],
                             [default_parse_interval(B1,1),B3]],A3,B3)
        #
        # original -> A3                B3
        # request  ->      A2     B2
        # interval -> empty
        # range    -> A3                B3
        self.interval(A2,B2,[],A3,B3)
        #
        # original ->          A3                B3
        # request  -> A4  B4
        # interval -> A4      A3-
        # range    -> A4                         B3
        A4   = mkdate(2010,4,20)
        B4   = mkdate(2010,5,1)
        self.interval(A4,B4,[[A4,default_parse_interval(A3,-1)]],A4,B3)
        #
        # original -> A4                         B3
        # request  ->                A2                  B5
        # interval ->                             B3+    B5
        # range    -> A4                                 B5
        B5   = mkdate(2010,6,1)
        self.interval(A2,B5,[[default_parse_interval(B3,1),B5]],A4,B5)
        #
        # original -> A4                                 B5
        # request  ->                                        A6    B6
        # interval ->                                     B5+      B6
        # range    -> A4                                           B6
        A6   = mkdate(2010,7,1)
        B6   = mkdate(2010,8,1)
        self.interval(A6,B6,[[default_parse_interval(B5,1),B6]],A4,B6)
        
    def testSetLen(self):
        ts = self.get()
        mkdate = self.mkdate
        dt = mkdate(2010,7,1)
        dt2 = mkdate(2010,4,1)
        ts.data.add(dt,56)
        ts.data[dt2] = 78
        ts.save()
        self.assertEqual(ts.data.size(),2)
        ts.data.update({mkdate(2009,3,1):"ciao",mkdate(2009,7,4):"luca"})
        ts.save()
        self.assertEqual(ts.data.size(),4)
        self.assertTrue(dt2 in ts.data)
        self.assertFalse(mkdate(2000,4,13) in ts.data)
        
    def testGet(self):
        ts = self.get()
        mkdate = self.mkdate
        dt = mkdate(2010,7,1)
        dt2 = mkdate(2010,4,1)
        ts.data.add(dt,56)
        ts.data[dt2] = 78
        ts.save()
        self.assertEqual(ts.data.get(dt),56)
        self.assertEqual(ts.data[dt2],78)
        self.assertRaises(KeyError,lambda : ts.data[mkdate(2010,3,1)])
        self.assertEqual(ts.data.get(mkdate(2010,3,1)),None)
        

class TestDateHashTimeSeries(TestHashTimeSeries):
    model = DateHashTimeSeries
    mkdate = date
    
    