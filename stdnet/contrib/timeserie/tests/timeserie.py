from itertools import izip
from datetime import date
from random import uniform

from stdnet.test import TestCase
from stdnet.contrib.timeserie.utils import dategenerator, default_parse_interval
from stdnet.utils import populate

from models import TimeSerie


NUM_DATES = 300

dates    = populate('date',NUM_DATES)
values   = populate('float',NUM_DATES, start = 10, end = 400)
alldata  = list(izip(dates,values))
testdata = dict(alldata)


class TestTimeSerie(TestCase):
    
    def setUp(self):
        self.orm.register(TimeSerie)
        TimeSerie(ticker = 'GOOG').save()
        
    def get(self, ticker = 'GOOG'):
        return TimeSerie.objects.get(ticker = ticker)
        
    def filldata(self):
        d = self.get()
        d.data.update(testdata)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(testdata))
        return self.get()

    def interval(self, a, b, targets, C, D):
        ts = self.get()
        intervals = ts.intervals(a,b)
        self.assertEqual(len(intervals),len(targets))
        for interval,target in izip(intervals,targets):
            x = interval[0]
            y = interval[1]
            self.assertEqual(x,target[0])
            self.assertEqual(y,target[1])
            for dt in dategenerator(x,y):
                ts.data.add(dt,uniform(0,1))
        ts.storestartend()
        self.assertEqual(ts.start,C)
        self.assertEqual(ts.end,D)
        
    def testkeys(self):
        ts = self.filldata()
        keyp = None
        data = testdata.copy()
        for key in ts.data.sortedkeys():
            if keyp:
                self.assertTrue(key,keyp)
            keyp = key
            data.pop(key)
        self.assertEqual(len(data),0)
        
    def testitems(self):
        ts = self.filldata()
        keyp = None
        data = testdata.copy()
        for key,value in ts.data.sorteditems():
            if keyp:
                self.assertTrue(key,keyp)
            keyp = key
            self.assertEqual(data.pop(key),value)
        self.assertEqual(len(data),0)
        
    def testInterval(self):
        '''Test interval handling'''
        ts = self.get()
        self.assertEqual(ts.start,None)
        self.assertEqual(ts.end,None)
        #
        #
        A1   = date(2010,5,10)
        B1   = date(2010,5,12)
        self.interval(A1,B1,[[A1,B1]],A1,B1)
        #
        #  original ->      A1      B1
        #  request  -> A2      B2
        #  interval -> A2  A1-
        #  range    -> A2          B1
        A2   = date(2010,5,6)
        B2   = date(2010,5,11)
        self.interval(A2,B2,[[A2,default_parse_interval(A1,-1)]],A2,B1)
        #
        #  original ->      A2      B1
        #  request  -> A3                B3
        #  interval -> A3  A2-       B1+ B3
        #  range    -> A3                B3
        A3   = date(2010,5,4)
        B3   = date(2010,5,14)
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
        A4   = date(2010,4,20)
        B4   = date(2010,5,1)
        self.interval(A4,B4,[[A4,default_parse_interval(A3,-1)]],A4,B3)
        #
        # original -> A4                         B3
        # request  ->                A2                  B5
        # interval ->                             B3+    B5
        # range    -> A4                                 B5
        B5   = date(2010,6,1)
        self.interval(A2,B5,[[default_parse_interval(B3,1),B5]],A4,B5)
        #
        # original -> A4                                 B5
        # request  ->                                        A6    B6
        # interval ->                                     B5+      B6
        # range    -> A4                                           B6
        A6   = date(2010,7,1)
        B6   = date(2010,8,1)
        self.interval(A6,B6,[[default_parse_interval(B5,1),B6]],A4,B6)
        
    def testGetSet(self):
        ts = self.get()
        dt = date(2010,7,1)
        dt2 = date(2010,4,1)
        ts.data.add(dt,56)
        ts.data[dt2] = 78
        ts.save()
        self.assertEqual(ts.data.get(dt),56)
        self.assertEqual(ts.data[dt2],78)
        try:
            ts.data[date(2010,3,1)]
        except KeyError:
            pass
        else:
            self.fail('KeyError')
        self.assertEqual(ts.data.get(date(2010,3,1)),None)
        
