import os
from random import randint
from datetime import date, datetime, timedelta
from struct import unpack

from stdnet import SessionNotAvailable, CommitException
from stdnet.utils import test, encoders, populate, ispy3k, iteritems
from stdnet.apps.columnts import ColumnTS, as_dict
from stdnet.backends import redisb

from tests.all.structures.base import StructMixin


nan = float('nan')
this_path = os.path.split(os.path.abspath(__file__))[0]

bin_to_float = lambda f : unpack('>d', f)[0]
if ispy3k:  # pragma nocover
    bitflag = lambda value: value
else:   # pragma nocover
    bitflag = ord

class timeseries_test1(redisb.RedisScript):
    script = (redisb.read_lua_file('tabletools'),
              redisb.read_lua_file('columnts.columnts'),
              redisb.read_lua_file('test1',this_path))


class ColumnData(test.DataGenerator):
    sizes = {'tiny': 100,
             'small': 300,
             'normal': 2000,
             'big': 10000,
             'huge': 1000000}

    def generate(self):
        size = self.size
        self.data1 = tsdata(self, ('a','b','c','d','f','g'))
        self.data2 = tsdata(self, ('a','b','c','d','f','g'))
        self.data3 = tsdata(self, ('a','b','c','d','f','g'))
        self.missing = tsdata(self, ('a','b','c','d','f','g'), missing=True)
        self.data_mul1 = tsdata(self, ('eurusd',))
        self.data_mul2 = tsdata(self, ('gbpusd',))


class tsdata(object):

    def __init__(self, g, fields, start=None, end=None, missing=False):
        end = end or date.today()
        if not start:
            start = end - timedelta(days=g.size)
        # random dates
        self.dates = g.populate('date', start=start, end=end)
        self.unique_dates = set(self.dates)
        self.fields = {}
        self.sorted_fields = {}
        for field in fields:
            vals = g.populate('float')
            if missing:
                N = len(vals)
                for num in range(randint(0, N//2)):
                    index = randint(0, N-1)
                    vals[index] = nan
            self.fields[field] = vals
            self.sorted_fields[field] = []
        self.values = []
        date_dict = {}
        for i,dt in enumerate(self.dates):
            vals = dict(((f,v[i]) for f,v in iteritems(self.fields)))
            self.values.append((dt,vals))
            date_dict[dt] = vals
        sdates = []
        for i,dt in enumerate(sorted(date_dict)):
            sdates.append(dt)
            fields = date_dict[dt]
            for field in fields:
                self.sorted_fields[field].append(fields[field])
        self.sorted_values = (sdates,self.sorted_fields)
        self.length = len(sdates)

    def create(self, test, id=None):
        '''Create one ColumnTS with six fields and cls.size dates'''
        models = test.mapper
        ts = models.register(test.structure())
        models.session().add(ts)
        with ts.session.begin() as t:
            t.add(ts)
            ts.update(self.values)
        yield t.on_result
        yield ts


class ColumnMixin(object):
    '''Used by all tests on ColumnTS'''
    structure = ColumnTS
    name = 'columnts'
    data_cls = ColumnData

    def create_one(self):
        ts = self.structure()
        d1 = date(2012,1,23)
        data = {d1: {'open':586, 'high':588.66,
                     'low':583.16, 'close':585.52},
                date(2012,1,20): {'open':590.53, 'high':591,
                                  'low':581.7, 'close':585.99},
                date(2012,1,19): {'open':640.99, 'high':640.99,
                                  'low':631.46, 'close':639.57}}
        ts.add(d1, data[d1])
        self.data = data
        data = self.data.copy()
        data.pop(d1)
        data = tuple(data.items())
        ts.update(data)
        # test bad add
        self.assertRaises(TypeError, ts.add, date(2012,1,20), 1, 2, 3)
        return ts

    def empty(self):
        models = self.mapper
        l = models.register(self.structure())
        self.assertTrue(l.id)
        models.session().add(l)
        self.assertTrue(l.session is not None)
        return l

    def check_stats(self, stat_field, data):
        N = len(data)
        cdata = list((d for d in data if d==d))
        cdata2 = list((d*d for d in cdata))
        dd = list((a-b for a,b in zip(cdata[1:],cdata[:-1])))
        dd2 = list((d*d for d in dd))
        NC = len(cdata)
        self.assertEqual(stat_field['N'],NC)
        self.assertAlmostEqual(stat_field['min'], min(cdata))
        self.assertAlmostEqual(stat_field['max'], max(cdata))
        self.assertAlmostEqual(stat_field['sum'], sum(cdata)/NC)
        self.assertAlmostEqual(stat_field['sum2'], sum(cdata2)/NC)
        self.assertAlmostEqual(stat_field['dsum'], sum(dd)/(NC-1))
        self.assertAlmostEqual(stat_field['dsum2'], sum(dd2)/(NC-1))

    def as_dict(self, serie):
        times, fields = yield serie.irange()
        yield as_dict(times, fields)

    def makeGoogle(self):
        ts = self.mapper.register(self.create_one())
        self.assertTrue(len(ts.cache.fields['open']), 2)
        self.assertTrue(len(ts.cache.fields), 4)
        yield self.mapper.session().add(ts)
        yield self.async.assertEqual(ts.size(), 3)
        dates, fields = yield ts.irange()
        self.assertEqual(len(fields), 4)
        self.assertEqual(len(dates), 3)
        for field in fields:
            values = fields[field]
            self.assertEqual(len(values), 3)
            for dt, v in zip(dates, values):
                v2 = self.data[dt.date()][field]
                self.assertAlmostEqual(v, v2)
        yield ts


class TestTimeSeries(ColumnMixin, StructMixin, test.TestCase):

    def testLuaClass(self):
        ts = self.empty()
        backend = ts.backend_structure()
        self.assertEqual(backend.instance, ts)
        c = backend.client
        r = yield c.execute_script('timeseries_test1', (backend.id,))
        self.assertEqual(r, b'OK')

    def testEmpty2(self):
        '''Check an empty timeseries'''
        ts = self.empty()
        yield self.async.assertEqual(ts.numfields(), 0)
        yield self.async.assertEqual(ts.fields(), ())

    def testFrontBack(self):
        models = self.mapper
        ts = models.register(ColumnTS(pickler=encoders.DateConverter()))
        models.session().add(ts)
        yield self.async.assertEqual(ts.front(), None)
        yield self.async.assertEqual(ts.back(), None)
        d2 = date.today()
        d1 = d2 - timedelta(days=2)
        with ts.session.begin() as t:
            ts.add(d2,'foo',-5.2)
            ts.add(d1,'foo',789.3)
        yield t.on_result
        yield self.async.assertEqual(ts.size(),2)
        yield self.async.assertEqual(ts.front(), (d1, {'foo':789.3}))
        yield self.async.assertEqual(ts.back(), (d2, {'foo':-5.2}))

    def test_ddd_simple(self):
        ts = self.empty()
        with ts.session.begin() as t:
            ts.add(date.today(), 'pv', 56)
            self.assertTrue(ts.cache.fields)
            ts.add(date.today()-timedelta(days=2), 'pv', 53.8)
            self.assertTrue(len(ts.cache.fields['pv']), 2)
        yield t.on_result
        yield self.async.assertEqual(ts.fields(), ('pv',))
        yield self.async.assertEqual(ts.numfields(), 1)
        yield self.async.assertEqual(ts.size(), 2)
        #
        # Check that a string is available at the field key
        bts = ts.backend_structure()
        keys = yield bts.allkeys()
        keys = tuple((b.decode('utf-8') for b in keys))
        self.assertEqual(len(keys), 3)
        self.assertTrue(bts.id in keys)
        self.assertTrue(bts.fieldsid in keys)
        self.assertTrue(bts.fieldid('pv') in keys)
        raw_data = bts.field('pv')
        self.assertTrue(raw_data)
        self.assertEqual(len(raw_data),18)
        a1 = raw_data[:9]
        a2 = raw_data[9:]
        n = bitflag(a1[0])
        self.assertEqual(n, bitflag(a2[0]))
        self.assertEqual(n, 2)
        self.assertEqual(bin_to_float(a1[1:]), 53.8)
        self.assertEqual(bin_to_float(a2[1:]), 56)
        #
        data = ts.irange()
        self.assertEqual(len(data),2)
        dt,fields = data
        self.assertEqual(len(dt),2)
        self.assertTrue('pv' in fields)
        for v, t in zip(fields['pv'],[53.8, 56]):
            self.assertAlmostEqual(v, t)

    def test_add_nil(self):
        ts = self.empty()
        with ts.session.begin() as t:
            ts.add(date.today(), 'pv', 56)
            ts.add(date.today()-timedelta(days=2), 'pv', nan)
        yield t.on_result
        yield self.async.assertEqual(ts.size(), 2)
        dt, fields = yield ts.irange()
        self.assertEqual(len(dt), 2)
        self.assertTrue('pv' in fields)
        n = fields['pv'][0]
        self.assertNotEqual(n, n)

    def testGoogleDrop(self):
        ts = yield self.makeGoogle()
        yield self.async.assertEqual(ts.fields(), ('close','high','low','open'))
        yield self.async.assertEqual(ts.numfields(), 4)
        yield self.async.assertEqual(ts.size(), 3)

    def testRange(self):
        ts = yield self.makeGoogle()
        data = ts.irange()
        self.assertEqual(len(data),2)
        dt,fields = data
        self.assertEqual(len(fields),4)
        high = list(zip(dt,fields['high']))
        self.assertEqual(high[0],(datetime(2012,1,19),640.99))
        self.assertEqual(high[1],(datetime(2012,1,20),591))
        self.assertEqual(high[2],(datetime(2012,1,23),588.66))

    def testRangeField(self):
        ts = yield self.makeGoogle()
        data = ts.irange(fields=('low','high','badone'))
        self.assertEqual(len(data),2)
        dt,fields = data
        self.assertEqual(len(fields),2)
        low = list(zip(dt,fields['low']))
        high = list(zip(dt,fields['high']))
        self.assertEqual(high[0],(datetime(2012,1,19),640.99))
        self.assertEqual(high[1],(datetime(2012,1,20),591))
        self.assertEqual(high[2],(datetime(2012,1,23),588.66))

    def testRaises(self):
        ts = yield self.makeGoogle()
        self.assertRaises(TypeError, ts.merge, 5)
        self.assertRaises(ValueError, ts.merge, (5,))
        ts.session = None
        self.assertRaises(SessionNotAvailable, ts.merge, (5, ts))

    def testUpdateDict(self):
        '''Test updating via a dictionary.'''
        ts = yield self.makeGoogle()
        data = {date(2012,1,23):{'open':586.00, 'high':588.66,
                                 'low':583.16, 'close':585.52},
                date(2012,1,25):{'open':586.32, 'high':687.68,
                                 'low':578, 'close':580.93},
                date(2012,1,24):{'open':586.32, 'high':687.68,
                                 'low':578, 'close':580.93}}
        ts.update(data)
        self.assertEqual(ts.size(), 5)
        dates, fields = ts.range(date(2012,1,23), date(2012,1,25))
        self.assertEqual(len(dates),3)
        self.assertEqual(dates[0].date(),date(2012,1,23))
        self.assertEqual(dates[1].date(),date(2012,1,24))
        self.assertEqual(dates[2].date(),date(2012,1,25))
        for field in fields:
            for d, v1 in zip(dates, fields[field]):
                v2 = data[d.date()][field]
                self.assertAlmostEqual(v1, v2)

    def __testBadQuery(self):
        ts = yield self.makeGoogle()
        # get the backend id and override it
        id = ts.dbid()
        client = ts.session.backend.client
        client.delete(id)
        client.rpush(id, 'bla')
        client.rpush(id, 'foo')
        self.assertEqual(client.llen(id), 2)
        self.assertRaises(redisb.ScriptError, ts.add,
                          date(2012,1,23), {'open':586})
        self.assertRaises(redisb.ScriptError, ts.irange)
        self.assertRaises(redisb.RedisInvalidResponse, ts.size)

    def test_get(self):
        ts = yield self.makeGoogle()
        v = yield ts.get(date(2012,1,23))
        self.assertTrue(v)
        self.assertEqual(len(v),4)
        v2 = ts[date(2012,1,23)]
        self.assertEqual(v,v2)
        self.assertEqual(ts.get(date(2014,1,1)),None)
        self.assertRaises(KeyError, lambda: ts[date(2014,1,1)])

    def testSet(self):
        ts = yield self.makeGoogle()
        ts[date(2012,1,27)] = {'open': 600}
        self.assertEqual(len(ts), 4)
        res = ts[date(2012,1,27)]
        self.assertEqual(len(res),4)
        self.assertEqual(res['open'], 600)
        self.assertNotEqual(res['close'],res['close'])
        self.assertNotEqual(res['high'],res['high'])
        self.assertNotEqual(res['low'],res['low'])

    def test_times(self):
        ts = yield self.makeGoogle()
        dates = yield ts.itimes()
        self.assertTrue(dates)
        self.assertEqual(len(dates), 3)
        for dt in dates:
            self.assertIsInstance(dt, datetime)


class TestOperations(ColumnMixin, test.TestCase):

    @classmethod
    def after_setup(cls):
        cls.ts1 = yield cls.data.data1.create(cls)
        cls.ts2 = yield cls.data.data2.create(cls)
        cls.ts3 = yield cls.data.data3.create(cls)
        cls.mul1 = yield cls.data.data_mul1.create(cls)
        cls.mul2 = yield cls.data.data_mul2.create(cls)

    def test_merge2series(self):
        data = self.data
        ts1, ts2 = self.ts1, self.ts2
        yield self.async.assertEqual(ts1.size(), data.data1.length)
        yield self.async.assertEqual(ts1.numfields(), 6)
        yield self.async.assertEqual(ts2.size(), data.data2.length)
        yield self.async.assertEqual(ts2.numfields(), 6)
        ts3 = self.mapper.register(self.structure())
        session = self.mapper.session()
        with session.begin() as t:
            t.add(ts3)
            # merge ts1 with weight -1  and ts2 with weight 2
            ts3.merge((-1, ts1), (2, ts2))
        yield t.on_result
        yield self.async.assertTrue(ts3.size())
        yield self.async.assertEqual(ts3.numfields(), 6)
        times, fields = ts3.irange()
        for i,dt in enumerate(times):
            dt = dt.date()
            v1 = ts1.get(dt)
            v2 = ts2.get(dt)
            if dt in data.data1.unique_dates and dt in data.data2.unique_dates:
                for field, values in fields.items():
                    res = 2*v2[field] - v1[field]
                    self.assertAlmostEqual(values[i],res)
            else:
                self.assertTrue(v1 is None or v2 is None)
                for values in fields.values():
                    v = values[i]
                    self.assertNotEqual(v,v)

    def test_merge3series(self):
        data = self.data
        ts1, ts2, ts3 = self.ts1, self.ts2, self.ts3
        ts4 = self.mapper.register(self.structure())
        session = self.mapper.session()
        yield self.async.assertEqual(ts1.size(), data.data1.length)
        yield self.async.assertEqual(ts2.size(), data.data2.length)
        yield self.async.assertEqual(ts3.size(), data.data3.length)
        with session.begin() as t:
            t.add(ts4)
            # merge ts1 with weight -1  and ts2 with weight 2
            ts4.merge((0.5, ts1), (1.3, ts2), (-2.65, ts3))
            self.assertEqual(ts4.session, session)
        yield t.on_result
        length = yield ts4.size()
        self.assertTrue(length >= max(data.data1.length, data.data2.length,
                                      data.data3.length))
        yield self.async.assertEqual(ts2.numfields(), 6)
        #
        results = yield self.as_dict(ts4)
        d1 = yield self.as_dict(ts1)
        d2 = yield self.as_dict(ts2)
        d3 = yield self.as_dict(ts3)
        #
        for dt in results:
            v1 = d1.get(dt)
            v2 = d2.get(dt)
            v3 = d3.get(dt)
            result = results[dt]
            if v1 is not None and v2 is not None and v3 is not None:
                for field in result:
                    vc = result[field]
                    res = 0.5*v1[field] + 1.3*v2[field] - 2.65*v3[field]
                    self.assertAlmostEqual(vc, res)
            else:
                for v in result.values():
                    self.assertNotEqual(v, v)

    def test_add_multiply1(self):
        data = self.data
        ts1, ts2, mul1 = self.ts1, self.ts2, self.mul1
        ts = self.mapper.register(self.structure())
        session = self.mapper.session()
        with session.begin() as t:
            t.add(ts)
            ts.merge((1.5, mul1, ts1), (-1.2, ts2))
            self.assertTrue(ts.cache.merged_series)
            self.assertEqual(ts.session, session)
        yield t.on_result
        length = yield ts.size()
        self.assertTrue(length >= max(data.data1.length, data.data2.length))
        yield self.async.assertEqual(ts.numfields(), 6)
        results = yield self.as_dict(ts)
        mul1 = yield self.as_dict(mul1)
        d1 = yield self.as_dict(ts1)
        d2 = yield self.as_dict(ts2)
        for dt in results:
            v1 = d1.get(dt)
            v2 = d2.get(dt)
            m1 = mul1.get(dt)
            result = results[dt]
            if v1 is not None and v2 is not None and m1 is not None:
                m1 = m1['eurusd']
                for field in result:
                    vc = result[field]
                    res = 1.5*m1*v1[field] - 1.2*v2[field]
                    self.assertAlmostEqual(vc, res)
            else:
                for v in result.values():
                    self.assertNotEqual(v,v)

    def test_add_multiply2(self):
        data = self.data
        ts1, ts2, mul1, mul2 = self.ts1, self.ts2, self.mul1, self.mul2
        ts = self.mapper.register(self.structure())
        session = self.mapper.session()
        with session.begin() as t:
            t.add(ts)
            ts.merge((1.5, mul1, ts1), (-1.2, mul2, ts2))
            self.assertEqual(ts.session, session)
        yield t.on_result
        length = yield ts.size()
        self.assertTrue(length >= max(data.data1.length, data.data2.length))
        yield self.async.assertEqual(ts.numfields(), 6)
        times, fields = ts.irange()
        for i,dt in enumerate(times):
            dt = dt.date()
            v1 = ts1.get(dt)
            v2 = ts2.get(dt)
            m1 = mul1.get(dt)
            m2 = mul2.get(dt)
            if v1 is not None and v2 is not None and m1 is not None\
                     and m2 is not None:
                m1 = m1['eurusd']
                m2 = m2['gbpusd']
                for field,values in fields.items():
                    res = 1.5*m1*v1[field] - 1.2*m2*v2[field]
                    self.assertAlmostEqual(values[i],res)
            else:
                for values in fields.values():
                    v = values[i]
                    self.assertNotEqual(v,v)

    def test_multiply_no_store(self):
        data = self.data
        ts1, ts2 = self.ts1, self.ts2
        times, fields = yield self.structure.merged_series((1.5, ts1),
                                                           (-1.2, ts2))
        for i,dt in enumerate(times):
            dt = dt.date()
            v1 = ts1.get(dt)
            v2 = ts2.get(dt)
            if v1 is not None and v2 is not None:
                for field,values in fields.items():
                    res = 1.5*v1[field] - 1.2*v2[field]
                    self.assertAlmostEqual(values[i],res)
            else:
                for values in fields.values():
                    v = values[i]
                    self.assertNotEqual(v,v)

    def test_merge_fields(self):
        data = self.data
        ts1, ts2, mul1, mul2 = self.ts1, self.ts2, self.mul1, self.mul2
        ts = self.mapper.register(self.structure())
        session = self.mapper.session()
        with session.begin() as t:
            t.add(ts)
            ts.merge((1.5, mul1, ts1), (-1.2, mul2, ts2),
                     fields=('a','b','c','badone'))
            self.assertEqual(ts.session,session)
        yield t.on_result
        length = yield ts.size()
        self.assertTrue(length >= max(data.data1.length, data.data2.length))
        yield self.async.assertEqual(ts.numfields(), 3)
        yield self.async.assertEqual(ts.fields(), ('a','b','c'))
        times, fields = yield ts.irange()
        for i,dt in enumerate(times):
            dt = dt.date()
            v1 = ts1.get(dt)
            v2 = ts2.get(dt)
            m1 = mul1.get(dt)
            m2 = mul2.get(dt)
            if v1 is not None and v2 is not None and m1 is not None\
                     and m2 is not None:
                m1 = m1['eurusd']
                m2 = m2['gbpusd']
                for field,values in fields.items():
                    res = 1.5*m1*v1[field] - 1.2*m2*v2[field]
                    self.assertAlmostEqual(values[i],res)
            else:
                for values in fields.values():
                    v = values[i]
                    self.assertNotEqual(v,v)


class a:
#class TestMissingValues(TestOperations):

    @classmethod
    def after_setup(cls):
        cls.ts1 = yield cls.data.missing.create(cls)

    def test_missing(self):
        result = self.ts1.istats(0, -1)
        stats = result['stats']
        self.assertEqual(len(stats), 6)
        for stat in stats:
            self.check_stats(stats[stat],self.fields[stat])

