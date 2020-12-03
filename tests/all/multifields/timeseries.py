"""tests for odm.TimeSeriesField"""
import os
from datetime import date, datetime
from random import uniform

from examples.tsmodels import DateTimeSeries, TimeSeries

from stdnet import odm
from stdnet.utils import dategenerator, default_parse_interval, test, todate, zip

from .struct import MultiFieldMixin


class TsData(test.DataGenerator):
    def generate(self):
        self.dates = self.populate("date")
        self.values = self.populate("float", start=10, end=400)
        self.dates2 = self.populate(
            "date", start=date(2009, 1, 1), end=date(2010, 1, 1)
        )
        self.big_strings = self.populate(min_len=300, max_len=1000)
        self.alldata = list(zip(self.dates, self.values))
        self.alldata2 = list(zip(self.dates2, self.values))
        self.testdata = dict(self.alldata)
        self.testdata2 = dict(self.alldata2)


class TestDateTimeSeries(MultiFieldMixin, test.TestCase):
    multipledb = "redis"
    model = TimeSeries
    mkdate = datetime
    data_cls = TsData

    def defaults(self):
        return {"ticker": self.name}

    def adddata(self, obj, data=None):
        data = data or self.data.testdata
        yield obj.data.update(data)
        yield self.async.assertEqual(obj.data.size(), len(data))

    def make(self, name=None):
        return self.session().add(self.model(ticker=name or self.name))

    def get(self, name=None):
        name = name or self.name
        return self.query().get(ticker=name)

    def filldata(self, data=None, name=None):
        d = yield self.make(name=name)
        yield self.adddata(d, data)
        yield self.get(name=name)

    def interval(self, a, b, targets, C, D):
        ts = yield self.get()
        intervals = ts.intervals(a, b)
        self.assertEqual(len(intervals), len(targets))
        for interval, target in zip(intervals, targets):
            x = interval[0]
            y = interval[1]
            self.assertEqual(x, target[0])
            self.assertEqual(y, target[1])
            for dt in dategenerator(x, y):
                ts.data.add(dt, uniform(0, 1))
        self.assertEqual(ts.data_start, C)
        self.assertEqual(ts.data_end, D)

    def testFrontBack(self):
        ts = yield self.make()
        self.assertEqual(ts.data_start, None)
        self.assertEqual(ts.data_end, None)
        mkdate = self.mkdate
        ts.data.update(self.data.testdata2)
        start = ts.data_start
        end = ts.data_end
        p = start
        for d in ts.dates():
            self.assertTrue(d >= p)
            p = d
        self.assertEqual(d, end)

    def testkeys(self):
        ts = yield self.filldata()
        keyp = None
        data = self.data.testdata.copy()
        for key in ts.dates():
            if keyp:
                self.assertTrue(key, keyp)
            keyp = key
            data.pop(todate(key))
        self.assertEqual(len(data), 0)

    def testitems(self):
        ts = yield self.filldata()
        keyp = None
        data = self.data.testdata.copy()
        for key, value in ts.items():
            if keyp:
                self.assertTrue(key, keyp)
            keyp = key
            self.assertEqual(data.pop(todate(key)), value)
        self.assertEqual(len(data), 0)

    def testUpdate(self):
        ts = yield self.make()
        dt1 = self.mkdate(2010, 5, 6)
        dt2 = self.mkdate(2010, 6, 6)
        ts.data[dt1] = 56
        ts.data[dt2] = 88
        self.assertEqual(ts.data[dt1], 56)
        self.assertEqual(ts.data[dt2], 88)
        ts.data[dt1] = "ciao"
        self.assertEqual(ts.data[dt1], "ciao")

    def testInterval(self):
        """Test interval handling"""
        ts = yield self.make()
        mkdate = self.mkdate
        self.assertEqual(ts.data_start, None)
        self.assertEqual(ts.data_end, None)
        #
        #
        A1 = mkdate(2010, 5, 10)
        B1 = mkdate(2010, 5, 12)
        self.interval(A1, B1, [[A1, B1]], A1, B1)
        #
        #  original ->      A1      B1
        #  request  -> A2      B2
        #  interval -> A2  A1-
        #  range    -> A2          B1
        A2 = mkdate(2010, 5, 6)
        B2 = mkdate(2010, 5, 11)
        self.interval(A2, B2, [[A2, default_parse_interval(A1, -1)]], A2, B1)
        #
        #  original ->      A2      B1
        #  request  -> A3                B3
        #  interval -> A3  A2-       B1+ B3
        #  range    -> A3                B3
        A3 = mkdate(2010, 5, 4)
        B3 = mkdate(2010, 5, 14)
        self.interval(
            A3,
            B3,
            [[A3, default_parse_interval(A2, -1)], [default_parse_interval(B1, 1), B3]],
            A3,
            B3,
        )
        #
        # original -> A3                B3
        # request  ->      A2     B2
        # interval -> empty
        # range    -> A3                B3
        self.interval(A2, B2, [], A3, B3)
        #
        # original ->          A3                B3
        # request  -> A4  B4
        # interval -> A4      A3-
        # range    -> A4                         B3
        A4 = mkdate(2010, 4, 20)
        B4 = mkdate(2010, 5, 1)
        self.interval(A4, B4, [[A4, default_parse_interval(A3, -1)]], A4, B3)
        #
        # original -> A4                         B3
        # request  ->                A2                  B5
        # interval ->                             B3+    B5
        # range    -> A4                                 B5
        B5 = mkdate(2010, 6, 1)
        self.interval(A2, B5, [[default_parse_interval(B3, 1), B5]], A4, B5)
        #
        # original -> A4                                 B5
        # request  ->                                        A6    B6
        # interval ->                                     B5+      B6
        # range    -> A4                                           B6
        A6 = mkdate(2010, 7, 1)
        B6 = mkdate(2010, 8, 1)
        self.interval(A6, B6, [[default_parse_interval(B5, 1), B6]], A4, B6)

    def testSetLen(self):
        ts = yield self.make()
        mkdate = self.mkdate
        dt = mkdate(2010, 7, 1)
        dt2 = mkdate(2010, 4, 1)
        data = ts.data
        with ts.session.begin() as t:
            data.add(dt, 56)
            data[dt2] = 78
        yield t.on_result
        yield self.async.assertEqual(ts.data.size(), 2)
        yield ts.data.update({mkdate(2009, 3, 1): "ciao", mkdate(2009, 7, 4): "luca"})
        yield self.async.assertEqual(ts.data.size(), 4)
        yield self.async.assertTrue(dt2 in ts.data)
        yield self.async.assertFalse(mkdate(2000, 4, 13) in ts.data)

    def testGet(self):
        ts = yield self.make()
        mkdate = self.mkdate
        with ts.session.begin() as t:
            dt = mkdate(2010, 7, 1)
            dt2 = mkdate(2010, 4, 1)
            ts.data.add(dt, 56)
            ts.data[dt2] = 78
        yield t.on_result
        yield self.async.assertEqual(ts.size(), 2)
        yield self.async.assertEqual(ts.data.get(dt), 56)
        yield self.async.assertEqual(ts.data[dt2], 78)
        yield self.async.assertRaises(KeyError, lambda: ts.data[mkdate(2010, 3, 1)])
        yield self.async.assertEqual(ts.data.get(mkdate(2010, 3, 1)), None)

    def testRange(self):
        """Test the range (by time) command"""
        ts = yield self.filldata(testdata2)
        d1 = date(2009, 4, 1)
        d2 = date(2009, 11, 1)
        data = list(ts.data.range(d1, d2))
        self.assertTrue(data)

    def testloadrelated(self):
        yield self.make()
        session = self.session()
        with session.begin() as t:
            t.add(self.model(ticker=self.names[1]))
        yield t.on_result
        # we have now to instances
        qm = session.query(self.model)
        m1, m2 = yield qm.filter(ticker=self.names[:2]).all()
        self.assertEqual(m1.session, m2.session)
        with session.begin() as t:
            m1.data.update(self.data.testdata)
            m2.data.update(self.data.testdata2)
        yield t.on_result
        yield self.async.assertTrue(m1.size())
        yield self.async.assertTrue(m2.size())
        qs = yield qm.filter(ticker=self.names[:2]).load_related("data").all()
        for m in qs:
            self.assertTrue(m.data.cache.cache)

    def testitems2(self):
        ts = yield self.filldata(self.data.testdata2)
        for k, v in ts.data.items():
            self.assertEqual(v, self.data.testdata2[todate(k)])

    def testiRange(self):
        ts = yield self.filldata(self.data.testdata2)
        N = ts.data.size()
        self.assertTrue(N)
        a = int(N / 4)
        b = 3 * a
        r1 = list(ts.data.irange(0, a))
        r2 = list(ts.data.irange(a, b))
        r3 = list(ts.data.irange(b, -1))
        self.assertEqual(r1[-1], r2[0])
        self.assertEqual(r2[-1], r3[0])
        self.assertEqual(r1[0], ts.data.front())
        self.assertEqual(r3[-1], ts.data.back())
        T = len(r1) + len(r2) + len(r3)
        self.assertEqual(T, N + 2)
        self.assertEqual(len(r1), a + 1)
        self.assertEqual(len(r2), b - a + 1)
        self.assertEqual(len(r3), N - b)

    def __testiRangeTransaction(self):
        ts = self.filldata(self.data.testdata2)
        N = ts.data.size()
        self.assertTrue(N)
        a = int(N / 4)
        b = 3 * a
        with self.session().begin() as t:
            ts.data.irange(0, a, t)
            ts.data.irange(a, b, t)
            ts.data.irange(b, -1, t)
            ts.data.front(t)
            ts.data.back(t)
        c = lambda x: x if isinstance(x, date) else list(x)
        r1, r2, r3, front, back = [c(r) for r in t.get_result()]
        self.assertEqual(r1[-1], r2[0])
        self.assertEqual(r2[-1], r3[0])
        self.assertEqual(r1[0][0], front)
        self.assertEqual(r3[-1][0], back)
        T = len(r1) + len(r2) + len(r3)
        self.assertEqual(T, N + 2)
        self.assertEqual(len(r1), a + 1)
        self.assertEqual(len(r2), b - a + 1)
        self.assertEqual(len(r3), N - b)

    def testRange(self):
        ts = yield self.filldata(self.data.testdata2)
        N = ts.data.size()
        a = int(N / 4)
        b = 3 * a
        r1 = list(ts.data.irange(0, a))
        r2 = list(ts.data.irange(a, b))
        r3 = list(ts.data.irange(b, -1))
        r4 = list(ts.data.range(r2[0][0], r2[-1][0]))
        self.assertEqual(r4[0], r2[0])
        self.assertEqual(r4[-1], r2[-1])

    def __testRangeTransaction(self):
        ts = self.filldata(self.data.testdata2)
        N = ts.data.size()
        a = int(N / 4)
        b = 3 * a
        with self.session().begin() as t:
            ts.data.irange(0, a, t)
            ts.data.irange(a, b, t)
            ts.data.irange(b, -1, t)
        r1, r2, r3 = [list(r) for r in t.get_result()]
        with self.session().begin() as t:
            ts.data.range(r2[0][0], r2[-1][0], t)
        r4 = [list(r) for r in t.get_result()][0]
        self.assertEqual(r4[0], r2[0])
        self.assertEqual(r4[-1], r2[-1])

    def testCount(self):
        ts = yield self.filldata(self.data.testdata2)
        N = ts.data.size()
        a = int(N / 4)
        b = 3 * a
        r1 = list(ts.data.irange(0, a))
        r2 = list(ts.data.irange(a, b))
        r3 = list(ts.data.irange(b, -1))
        self.assertEqual(ts.data.count(r2[0][0], r2[-1][0]), b - a + 1)


class TestDateSeries(TestDateTimeSeries):
    model = DateTimeSeries
    mkdate = date
