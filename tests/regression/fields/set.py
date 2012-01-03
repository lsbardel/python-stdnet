from datetime import datetime
from itertools import chain

from stdnet import test, getdb, struct
from stdnet.utils import populate, zip, flatzset
from stdnet.lib import zset

from examples.models import Calendar, DateValue, Collection, Group

NUM_DATES = 100

dates = populate('date',NUM_DATES)
values = populate('string', NUM_DATES, min_len = 10, max_len = 120)


class TestSetStructure(test.TestCase):
    
    def testAdd(self):
        s = struct.set()
        s.update((1,2,3,4,5,5))
        s.save()
        self.assertEqual(s.size(),5)
    
    
class TestSetField(test.TestCase):
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
    tag  = 'zdiffstore'
    
    def rpy(self):
        return getdb().client
        
    def fill(self, key, *vals):
        rpy = self.rpy()
        vals = flatzset(zip([0]*len(vals),vals))
        rpy.zadd(key,*vals)
        
    def testDiffStore(self):
        rpy = self.rpy()
        self.fill('s1','a','b','c','d')
        self.fill('s2','a','b','c')
        r = rpy.zdiffstore('s3',('s1','s2'))
        self.assertEqual(rpy.zcard('s3'),1)
        
    
class TestOrderedSet(test.TestCase):
    model = Calendar
    models = (Calendar,DateValue)
        
    def fill(self, update = False):
        ts = Calendar(name = 'MyCalendar').save()
        with DateValue.objects.transaction() as t:
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
                

