from datetime import datetime

from stdnet import test, getdb
from stdnet.utils import populate, zip

from examples.models import Calendar, DateValue

NUM_DATES = 100

dates = populate('date',NUM_DATES)
values = populate('string', NUM_DATES, min_len = 10, max_len = 120)


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
        
    
class TestOrderedSet(test.TestCase):
    
    def setUp(self):
        self.orm.register(Calendar)
        self.orm.register(DateValue)
        
    def unregister(self):
        self.orm.unregister(Calendar)
        self.orm.unregister(DateValue)
        
    def fill(self):
        ts = Calendar(name = 'MyCalendar').save()
        for dt,value in zip(dates,values):
            ts.add(dt,value)
        ts.save()
        return ts
    
    def testAdd(self):
        self.fill()
        
    def testOrder(self):
        self.fill()
        ts = Calendar.objects.get(name = 'MyCalendar')
        self.assertEqual(ts.data.size(),NUM_DATES)
        dprec = None
        for event in ts.data:
            if dprec:
                self.assertTrue(event.dt >= dprec)
            dprec = event.dt    
                


