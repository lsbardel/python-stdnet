'''tests for odm.SetField'''
from datetime import datetime
from itertools import chain

from stdnet import getdb
from stdnet.utils import test, populate, zip

from examples.models import Calendar, DateValue, Collection, Group


class ZsetData(test.DataGenerator):
    
    def generate(self):
        self.dates = self.populate('date')
        self.values = self.populate('string', min_len=10, max_len=120)
    
    
class TestSetField(test.TestCase):
    models = (Collection, Group)
        
    def test_simple(self):
        m = yield self.session().add(self.model())
        yield m.numbers.add(1)
        yield m.numbers.update((1, 2, 3, 4, 5))
        yield self.async.assertEqual(m.numbers.size(), 5)
        
    
class TestOrderedSet(test.TestCase):
    multipledb = 'redis'
    models = (Calendar, DateValue)
    data_cls = ZsetData
    
    def fill(self, update=False):
        session = self.session()
        c = yield session.add(Calendar(name=self.data.random_string()))
        with session.begin() as t:
            for dt, value in zip(self.data.dates, self.data.values):
                t.add(DateValue(dt=dt, value=value))
        yield t.on_result
        items = t.saved[DateValue._meta]
        with session.begin() as t:
            if update:
                c.data.update(items)
            else:
                for value in items:
                    c.data.add(value)
        yield t.on_result
        yield c
    
    def test_add(self):
        return self.fill()
        
    def test_update(self):
        return self.fill(True)
        
    def test_order(self):
        c = yield self.fill()
        yield self.async.assertEqual(c.data.size(), self.data.size)
        dprec = None
        events = yield c.data.items()
        for event in events:
            if dprec:
                self.assertTrue(event.dt >= dprec)
            dprec = event.dt
    
    def test_rank(self):
        c = yield self.fill()
        data = c.data
        vals = yield data.items()
        self.assertEqual(vals, data.cache.cache)
        data.cache.clear()
        self.assertEqual(data.cache.cache, None)
        ranks = []
        for v in vals:
            ranks.append(data.rank(v))
        ranks = yield self.multi_async(ranks)
                            

