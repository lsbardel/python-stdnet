from itertools import izip
from datetime import date, datetime

import test_timeseries
from models import TimeSeriesMap

testdata  = test_timeseries.testdata
testdata2 = test_timeseries.testdata2



class TestDateTimeSeriesMap(test_timeseries.TestTimeSeries):
    model = TimeSeriesMap
    mkdate = date
    
    def testkeys(self):
        return
        ts = self.filldata()
        keyp = None
        data = testdata.copy()
        for key in ts.data.sortedkeys():
            if keyp:
                self.assertTrue(key,keyp)
            keyp = key
            data.pop(todate(key))
        self.assertEqual(len(data),0)
        
    def testitems(self):
        return
        ts = self.filldata()
        keyp = None
        data = testdata.copy()
        for key,value in ts.data.sorteditems():
            if keyp:
                self.assertTrue(key,keyp)
            keyp = key
            self.assertEqual(data.pop(todate(key)),value)
        self.assertEqual(len(data),0)
        
    def testInterval(self):
        '''Test interval handling'''
        return
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
        
    def testRange(self):
        ts = self.get()
        mkdate = self.mkdate
        ts.data.update(testdata2)
        ts.save()
        start = mkdate(2009,5,1)
        end = mkdate(2009,9,20)
        front = ts.data.front()
        back  = ts.data.back()
        for d in ts.data.keys():
            self.assertTrue(d>=front)
            front = d
        self.assertEqual(d,back)
        #range = ts.data.range(start,end)
        #p = start
        #for k,v in range:
        #    self.assetTrue(k>=p)
        #    p = k