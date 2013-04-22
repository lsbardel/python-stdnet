'''tests for odm.SetField'''
from datetime import datetime
from itertools import chain

from stdnet import getdb
from stdnet.utils import test, populate, zip, flatzset
from stdnet.lib import zset

from examples.models import Calendar, DateValue, Collection, Group

NUM_DATES = 100

dates = populate('date', NUM_DATES)
values = populate('string', NUM_DATES, min_len=10, max_len=120)
    
    
class TestSetField(test.TestCase):
    models = (Collection, Group)
    model = Collection
    
    def setUp(self):
        self.register()
        
    def testSimple(self):
        m = self.model().save()
        m.numbers.add(1)
        m.numbers.update((1,2,3,4,5))
        self.assertEqual(m.numbers.size(), 5)
        

class TestPythonZset(test.TestCase):
    
    def testAdd(self):
        s = zset()
        s.add(3,'ciao')
        s.add(4,'bla')
        self.assertEqual(len(s),2)
        s.add(-1,'bla')
        self.assertEqual(len(s),2)
        data = list(s)
        self.assertEqual(data[0][1],'bla')
        self.assertEqual(data[1][1],'ciao')
        
    
class TestOrderedSet(test.TestCase):
    multipledb = 'redis'
    model = Calendar
    models = (Calendar,DateValue)
    
    def setUp(self):
        self.register()
        
    def fill(self, update=False):
        ts = Calendar(name='MyCalendar').save()
        with DateValue.objects.session().begin() as t:
            for dt, value in zip(dates, values):
                t.add(DateValue(dt=dt, value=value))
        items = DateValue.objects.query()
        if update:
            ts.data.update(items)
        else:
            for value in items:
                ts.data.add(value)
        ts.save()
        return ts
    
    def testAdd(self):
        self.fill()
        
    def testUpdate(self):
        self.fill(True)
        
    def testOrder(self):
        self.fill()
        ts = Calendar.objects.get(name='MyCalendar')
        self.assertEqual(ts.data.size(), NUM_DATES)
        dprec = None
        for event in ts.data:
            if dprec:
                self.assertTrue(event.dt >= dprec)
            dprec = event.dt
    
    def testRank(self):
        s = self.fill()
        data = s.data
        vals = list(data.values())
        for v in vals:
            r = data.rank(v)
            self.assertEqual(vals.index(v), r)
        items = data.items()
        # this call should go get the cached values
        nvals = list(data.values())
        self.assertEqual(vals, nvals)
        
                

