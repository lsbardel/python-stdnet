from datetime import date, datetime
from decimal import Decimal

import stdnet
from stdnet import test
from stdnet.utils import populate, zip
from stdnet.exceptions import FieldError

from examples.models import TestDateModel, Statistics, Statistics2, Page

NUM_DATES = 100
names = populate('string',NUM_DATES, min_len = 5, max_len = 20)
dates = populate('date', NUM_DATES, start=date(2010,5,1), end=date(2010,6,1))


class TestAtomFields(test.TestCase):
    
    def setUp(self):
        self.orm.register(TestDateModel)
        self.meta = TestDateModel._meta
    
    def unregister(self):
        self.orm.unregister(TestDateModel)
        
    def create(self):
        for na,dt in zip(names,dates):
            m = TestDateModel(name = na, dt = dt)
            m.save(False)
        TestDateModel.commit()
            
    def testFilter(self):
        self.create()
        all = TestDateModel.objects.all()
        self.assertEqual(len(dates),all.count())
        N = 0
        done_dates = set()
        for dt in dates:
            if dt not in done_dates:
                done_dates.add(dt)
                elems = TestDateModel.objects.filter(dt = dt)
                N += elems.count()
                for elem in elems:
                    self.assertEqual(elem.dt,dt)
        self.assertEqual(all.count(),N)
        
    def testDelete(self):
        self.create()
        N = 0
        done_dates = set()
        for dt in dates:
            if dt not in done_dates:
                done_dates.add(dt)
                objs = TestDateModel.objects.filter(dt = dt)
                N += objs.count()
                objs.delete()
        all = TestDateModel.objects.all()
        self.assertEqual(all.count(),0)
        
        # The only key remianed is the ids key for the AutoField
        keys = self.meta.cursor.keys()
        self.assertEqual(len(keys),1)
        self.assertEqual(keys[0],self.meta.autoid())
        

class TestIntegerField(test.TestCase):
    
    def setUp(self):
        self.orm.register(Page)
    
    def unregister(self):
        self.orm.unregister(Page)
        
    def testDefaultValue(self):
        p = Page()
        self.assertEqual(p.in_navigation,1)
        p = Page(in_navigation = '4')
        self.assertEqual(p.in_navigation,'4')
        p.save()
        self.assertEqual(p.in_navigation,'4')
        p = Page.objects.get(id = p.id)
        self.assertEqual(p.in_navigation,4)
        
    def testNotValidated(self):
        p = Page().save()
        p = Page(in_navigation = 'bla')
        self.assertRaises(FieldError,p.save)
        
    def testZeroValue(self):
        p = Page(in_navigation = 0)
        self.assertEqual(p.in_navigation,0)
        p.save()
        self.assertEqual(p.in_navigation,0)
        p = Page.objects.get(id = p.id)
        self.assertEqual(p.in_navigation,0)
               

class TestJsonField(test.TestCase):
    
    def setUp(self):
        self.orm.register(Statistics)
    
    def unregister(self):
        self.orm.unregister(Statistics)
        
    def testMetaData(self):
        field = Statistics._meta.dfields['data']
        self.assertEqual(field.sep,None)
        
    def testCreate(self):
        mean = Decimal('56.4')
        started = date(2010,1,1)
        timestamp = datetime.now()
        a = Statistics(dt = date.today(), data = {'mean': mean,
                                                  'std': 5.78,
                                                  'started': started,
                                                  'timestamp':timestamp}).save()
        self.assertEqual(a.data['mean'],mean)
        a = Statistics.objects.get(id = a.id)
        self.assertEqual(len(a.data),4)
        self.assertEqual(a.data['mean'],mean)
        self.assertEqual(a.data['started'],started)
        self.assertEqual(a.data['timestamp'],timestamp)
        
    def testEmpty(self):
        a = Statistics(dt = date.today())
        self.assertEqual(a.data,{})
        a.save()
        self.assertEqual(a.data,{})
        a = Statistics.objects.get(id = a.id)
        self.assertEqual(a.data,{})
        

class TestJsonFieldSep(test.TestCase):
    
    def setUp(self):
        self.orm.register(Statistics2)
    
    def unregister(self):
        self.orm.unregister(Statistics2)
        
    def testMetaData(self):
        field = Statistics2._meta.dfields['data']
        self.assertEqual(field.sep,'__')
        
    def testCreate(self):
        mean = Decimal('56.4')
        started = date(2010,1,1)
        timestamp = datetime.now()
        a = Statistics2(dt = date.today(), data = {'stat__mean': mean,
                                                   'stat__std': 5.78,
                                                   'stat__secondary__ar': 0.1,
                                                   'date__started': started,
                                                   'date__timestamp':timestamp}).save()
        a = Statistics2.objects.get(id = a.id)
        self.assertEqual(len(a.data),2)
        stat = a.data['stat']
        dt   = a.data['date']
        self.assertEqual(len(stat),3)
        self.assertEqual(len(dt),2)

        self.assertEqual(stat['mean'],mean)
        self.assertEqual(stat['secondary']['ar'],0.1)
        self.assertEqual(dt['started'],started)
        self.assertEqual(dt['timestamp'],timestamp)
        
        
class TestErrorAtomFields(test.TestCase):
    
    def testNotRegistered(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.ModelNotRegistered,m.save)
        self.assertRaises(stdnet.ModelNotRegistered,m._meta.table)
    
    def testNotSaved(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.StdNetException,m.delete)    

