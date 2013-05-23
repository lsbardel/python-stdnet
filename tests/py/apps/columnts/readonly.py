from datetime import date

from stdnet.utils import test
from stdnet.apps.columnts import ColumnTS

from .main import ColumnMixin, nan


class TestReadOnly(ColumnMixin, test.TestCase):

    @classmethod
    def after_setup(cls):
        cls.ts1 = yield cls.data.data1.create(cls)
        cls.ts2 = yield cls.data.data2.create(cls)
        cls.ts3 = yield cls.data.data3.create(cls)
        cls.mul1 = yield cls.data.data_mul1.create(cls)
        cls.mul2 = yield cls.data.data_mul2.create(cls)
    
    def test_info_simple(self):
        ts = yield self.empty()
        info = yield ts.info()
        self.assertEqual(info['size'], 0)
        self.assertFalse('start' in info)
        d1 = date(2012, 5, 15)
        d2 = date(2012, 5, 16)
        yield ts.update({d1: {'open':605},
                         d2: {'open':617}})
        info = yield ts.info()
        self.assertEqual(info['size'], 2)
        self.assertEqual(info['fields']['open']['missing'], 0)
        self.assertEqual(info['start'].date(), d1)
        self.assertEqual(info['stop'].date(), d2)
        d3 = date(2012,5,14)
        d4 = date(2012,5,13)
        yield ts.update({d3: {'open':nan,'close':607},
                         d4: {'open':nan,'close':nan}})
        info = yield ts.info()
        self.assertEqual(info['size'], 4)
        self.assertEqual(info['start'].date(), d4)
        self.assertEqual(info['stop'].date(), d2)
        self.assertEqual(info['fields']['open']['missing'], 2)
        self.assertEqual(info['fields']['close']['missing'], 3)
    
    def test_istats(self):
        data = self.data
        ts1 = self.ts1
        dt,fields = yield ts1.irange()
        self.assertEqual(len(fields), 6)
        result = yield ts1.istats(0,-1)
        self.assertTrue(result)
        self.assertEqual(result['start'],dt[0])
        self.assertEqual(result['stop'],dt[-1])
        self.assertEqual(result['len'],len(dt))
        stats = result['stats']
        for field in ('a','b','c','d','f','g'):
            self.assertTrue(field in stats)
            stat_field = stats[field]
            res = data.data1.sorted_fields[field]
            self.check_stats(stat_field, res)
            
    def test_stats(self):
        data = self.data
        ts1 = self.ts1
        dt, fields = yield ts1.irange()
        self.assertEqual(len(fields), 6)
        size = len(dt)
        idx = size // 4
        dt = dt[idx:-idx]
        start = dt[0]
        end = dt[-1]
        # Perform the statistics between start and end
        result = yield ts1.stats(start, end)
        self.assertTrue(result)
        self.assertEqual(result['start'], start)
        self.assertEqual(result['stop'], end)
        self.assertEqual(result['len'], len(dt))
        stats = result['stats']
        for field in ('a','b','c','d','f','g'):
            self.assertTrue(field in stats)
            stat_field = stats[field]
            res = data.data1.sorted_fields[field][idx:-idx]
            self.check_stats(stat_field, res)
            
    def testSimpleMultiStats(self):
        ts1 = self.ts1
        dt,fields = yield ts1.irange()
        result = ts1.imulti_stats()
        self.assertTrue(result)
        self.assertEqual(result['type'],'multi')
        self.assertEqual(result['start'],dt[0])
        self.assertEqual(result['stop'],dt[-1])
        self.assertEqual(result['N'],len(dt))
        
    def __test(self):
        ts.update({date(2012,5,15): {'open':605},
                   date(2012,5,16): {'open':617}})
        self.assertEqual(ts.evaluate('return self:length()'), 2)
        self.assertEqual(ts.evaluate('return self:fields()'), [b'open'])
        #Return the change from last open with respect prevois open
        change = "return self:rank_value(-1,'open')-"\
                 "self:rank_value(-2,'open')"
        self.assertEqual(ts.evaluate(change), 12)
