from datetime import datetime

from stdnet import test, getdb
from stdnet.utils import populate, zip
from stdnet.lib import zset

from examples.models import Calendar, DateValue, Collection, Group

NUM_DATES = 100

dates = populate('date',NUM_DATES)
values = populate('string', NUM_DATES, min_len = 10, max_len = 120)


class TestSetStructure(test.TestModelBase):
    models = (Collection,Group)
    model = Collection
    
    def testSimple(self):
        m = self.model().save()
        m.numbers.add(1)
        m.numbers.update((1,2,3,4,5))
        self.assertEqual(len(m._local_transaction._cachepipes),1)
        m.save()
        self.assertEqual(m.numbers.size(),5)
        

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
        
    
class TestCommands(test.TestCase):
    
    def setUp(self):
        self.rpy = getdb().redispy
        
    def fill(self, key, *vals):
        pipe = self.rpy.pipeline()
        for x in vals:
            pipe.zadd(key,x,0)
        pipe.execute()
        
    def testDiffStore(self):
        rpy = self.rpy
        self.fill('a','a','b','c','d')
        self.fill('b','a','b','c')
        r = self.rpy.zdiffstore('c1',('a','b'))
        self.assertEqual(rpy.zcard('c1'),1)
        
    
class TestOrderedSet(test.TestModelBase):
    model = Calendar
    models = (Calendar,DateValue)
        
    def fill(self, update = False):
        ts = Calendar(name = 'MyCalendar').save()
        with DateValue.transaction() as t:
            for dt,value in zip(dates,values):
                DateValue(dt = dt,value = value).save(t)
        
        items = DateValue.objects.all()
        
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
        ts = Calendar.objects.get(name = 'MyCalendar')
        self.assertEqual(ts.data.size(),NUM_DATES)
        dprec = None
        for event in ts.data:
            if dprec:
                self.assertTrue(event.dt >= dprec)
            dprec = event.dt    
                

