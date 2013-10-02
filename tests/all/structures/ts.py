import os
from datetime import date

from stdnet import odm
from stdnet.utils import test, encoders, zip

from tests.all.multifields.timeseries import TsData

from .base import StructMixin


class TestTS(StructMixin, test.TestCase):
    structure = odm.TS
    data_cls = TsData
    name = 'ts'

    def create_one(self):
        ts = self.structure()
        ts.update(zip(self.data.dates, self.data.values))
        self.assertFalse(ts.cache.cache)
        self.assertTrue(ts.cache.toadd)
        self.assertFalse(ts.cache.toremove)
        return ts

    def test_empty2(self):
        ts = self.empty()
        yield self.async.assertEqual(ts.front(), None)
        yield self.async.assertEqual(ts.back(), None)

    def test_range(self):
        ts = yield self.not_empty()
        yield self.async.assertEqual(ts.size(), len(set(self.data.dates)))
        front = yield ts.front()
        back = yield ts.back()
        self.assertTrue(back[0] > front[0])
        all_dates = yield ts.itimes()
        N = len(all_dates)
        start = N // 4
        end = 3 * N // 4
        range = yield ts.range(all_dates[start], all_dates[end])
        self.assertTrue(range)
        for time, val in range:
            self.assertTrue(time>=front[0])
            self.assertTrue(time<=back[0])

    def test_get(self):
        ts = yield self.not_empty()
        dt1 = self.data.dates[0]
        val1 = yield ts[dt1]
        self.assertTrue(val1)
        yield self.async.assertEqual(ts.get(dt1), val1)
        yield self.async.assertEqual(ts.get(date(1990,1,1)),None)
        yield self.async.assertEqual(ts.get(date(1990,1,1),1),1)
        yield self.async.assertRaises(KeyError, lambda : ts[date(1990,1,1)])

    def test_pop(self):
        ts = yield self.not_empty()
        dt = self.data.dates[5]
        yield self.async.assertTrue(dt in ts)
        v = yield ts.pop(dt)
        self.assertTrue(v)
        yield self.async.assertFalse(dt in ts)
        yield self.async.assertRaises(KeyError, ts.pop, dt)
        yield self.async.assertEqual(ts.pop(dt,'bla'), 'bla')

    def test_rank_ipop(self):
        ts = yield self.not_empty()
        dt = self.data.dates[5]
        value = yield ts.get(dt)
        r = yield ts.rank(dt)
        all_dates = yield ts.itimes()
        self.assertEqual(all_dates[r].date(), dt)
        value2 = yield ts.ipop(r)
        self.assertEqual(value, value2)
        yield self.async.assertFalse(dt in ts)

    def test_pop_range(self):
        ts = yield self.not_empty()
        all_dates = yield ts.itimes()
        N = len(all_dates)
        start = N // 4
        end = 3 * N // 4
        range = yield ts.range(all_dates[start],all_dates[end])
        self.assertTrue(range)
        range2 = yield ts.pop_range(all_dates[start], all_dates[end])
        self.assertEqual(range, range2)
        all_dates = yield ts.itimes()
        all_dates = set(all_dates)
        self.assertTrue(all_dates)
        for dt,_ in range:
            self.assertFalse(dt in all_dates)
