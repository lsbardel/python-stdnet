import os
import json
import time
from datetime import date, datetime
from decimal import Decimal

import stdnet
from stdnet import test
from stdnet.utils import populate, zip, is_string, to_string, unichr, ispy3k
from stdnet.exceptions import FieldError

from examples.models import TestDateModel, Statistics, Statistics2,\
                            Page, SimpleModel, Environment, NumericData,\
                            DateData, Statistics3

NUM_DATES = 100
names = populate('string',NUM_DATES, min_len = 5, max_len = 20)
dates = populate('date', NUM_DATES, start=date(2010,5,1), end=date(2010,6,1))


class TestScalarBase(test.TestCase):
    model = None
    
    def register(self):
        self.orm.register(self.model)
        self.meta = self.model._meta
    
    def unregister(self):
        self.orm.unregister(self.model)


class TestAtomFields(TestScalarBase):
    model = TestDateModel
        
    def create(self):
        with TestDateModel.transaction() as t:
            for na,dt in zip(names,dates):
                TestDateModel(person = na, name = na, dt = dt).save(t)
            
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
        
        # The only key remaining is the ids key for the AutoField
        keys = self.cleankeys(self.meta)
        self.assertEqual(len(keys),1)
        self.assertEqual(keys[0],self.meta.autoid())
        

class TestCharFields(TestScalarBase):
    model = SimpleModel
    
    def testUnicode(self):
        unicode_string = unichr(500) + to_string('ciao') + unichr(300)
        m = self.model(code = unicode_string).save()
        code = m.todict()['code']
        if ispy3k:
            self.assertEqual(str(m),unicode_string)
        else:
            self.assertEqual(str(m),code)
        self.assertTrue(isinstance(code,bytes))
        self.assertEqual(code.decode('utf-8'),unicode_string)
        m = self.model.objects.get(id = m.id)
        self.assertEqual(m.code,unicode_string)
        
    
class TestNumericData(TestScalarBase):
    model = NumericData
        
    def testDefaultValue(self):
        d = NumericData(pv = 1.).save()
        self.assertAlmostEqual(d.pv,1.)
        self.assertAlmostEqual(d.vega,0.)
        self.assertAlmostEqual(d.delta,1.)
        self.assertEqual(d.gamma,None)
        
    def testDefaultValue2(self):
        d = NumericData(pv = 0., delta = 0.).save()
        self.assertAlmostEqual(d.pv,0.)
        self.assertAlmostEqual(d.vega,0.)
        self.assertAlmostEqual(d.delta,0.)
        self.assertEqual(d.gamma,None)
        
    def testFieldError(self):
        self.assertRaises(stdnet.FieldError,NumericData().save)
                
        
class TestIntegerField(TestScalarBase):
    model = Page
            
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
               

class TestDateData(TestScalarBase):
    model = DateData
        
    def testDateindateTime(self):
        v = DateData(dt2 = date.today()).save()
        v = DateData.objects.get(id = v.id)
        self.assertEqual(v.dt1,None)
        self.assertEqual(v.dt2.date(),date.today())
        

class TestBoolField(TestScalarBase):
    model = NumericData
    
    def testMeta(self):
        self.assertEqual(len(self.meta.indices),1)
        index = self.meta.indices[0]
        self.assertEqual(index.type,'bool')
        self.assertEqual(index.scorefun(True),1)
        self.assertEqual(index.scorefun(False),0)
        
    def testBoolValue(self):
        d = self.model(pv = 1.).save()
        d = self.model.objects.get(id = d.id)
        self.assertEqual(d.ok,False)
        d.ok = 'jasxbhjaxsbjxsb'
        self.assertRaises(ValueError,d.save)
        d.ok = True
        d.save()
        d = self.model.objects.get(id = d.id)
        self.assertEqual(d.ok,True)
        
    
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
        
    def testCreateFromString(self):
        mean = 'mean'
        timestamp = time.time()
        data = {'mean': mean,
                'std': 5.78,
                'timestamp':timestamp}
        datas = json.dumps(data)
        a = Statistics(dt = date.today(), data = datas).save()
        a = Statistics.objects.get(id = a.id)
        self.assertEqual(a.data['mean'],mean)
        a = Statistics.objects.get(id = a.id)
        self.assertEqual(len(a.data),3)
        self.assertEqual(a.data['mean'],mean)
        self.assertEqual(a.data['timestamp'],timestamp)
        
    def testEmpty(self):
        a = Statistics(dt = date.today())
        self.assertEqual(a.data,{})
        a.save()
        self.assertEqual(a.data,{})
        a = Statistics.objects.get(id = a.id)
        self.assertEqual(a.data,{})
        

class TestJsonFieldSep(test.TestCase):
    
    def register(self):
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
        a = Statistics2(dt = date.today(),
                        data = {'stat__mean': mean,
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
        

class TestJsonFieldAsData(TestScalarBase):
    '''Test a model with a JSONField which expand as instance fields.'''
    model = Statistics3
    
    def make(self):
        data = {'mean': 1.0,
                'std': 5.78,
                'pv': 3.2,
                'name': 'bla',
                'dt': date.today()}
        return self.model(name = 'bla', data = data)
        
    def testMeta(self):
        field = self.meta.dfields['data']
        self.assertFalse(field.as_string)
        
    def testMake(self):
        m = self.make()
        self.assertTrue(m.is_valid())
        data = m.cleaned_data
        self.assertEqual(len(data),6)
        self.assertEqual(float(data['data__mean']),1.0)
        self.assertEqual(float(data['data__std']),5.78)
        self.assertEqual(float(data['data__pv']),3.2)
        
    def testGet(self):
        m = self.make().save()
        m = self.model.objects.get(id = m.id)
        self.assertEqual(m.data['mean'],1.0)
        self.assertEqual(m.data['std'],5.78)
        self.assertEqual(m.data['pv'],3.2)
        self.assertEqual(m.data['dt'],date.today())
        self.assertEqual(m.data['name'],'bla')
        
    
class TestByteField(test.TestCase):
    
    def setUp(self):
        self.orm.register(SimpleModel)
    
    def unregister(self):
        self.orm.unregister(SimpleModel)
        
    def testMetaData(self):
        field = SimpleModel._meta.dfields['somebytes']
        
    def testValue(self):
        v = SimpleModel(code='one', somebytes=to_string('hello'))
        self.assertTrue(is_string(v.somebytes))
        v.save()
        v = SimpleModel.objects.get(code = 'one')
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes,b'hello')
        
    def testValueByte(self):
        b = os.urandom(8)
        v = SimpleModel(code='one', somebytes=b)
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes,b)
        v.save()
        v = SimpleModel.objects.get(code = 'one')
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes,b)


class TestPickleObjectField(test.TestCase):
    
    def setUp(self):
        self.orm.register(Environment)
    
    def unregister(self):
        self.orm.unregister(Environment)
        
    def testOkObject(self):
        v = Environment(data = ['ciao','pippo']).save()
        self.assertEqual(v.data, ['ciao','pippo'])
        v = Environment.objects.get(id = v.id)
        self.assertEqual(v.data, ['ciao','pippo'])
        
    def testRecursive(self):
        '''Silly test to test both pickle field and pickable instace'''
        v = Environment(data = ('ciao','pippo', 4, {})).save()
        v2 = Environment(data = v).save()
        v3 = Environment.objects.get(id = v2.id)
        self.assertEqual(v3.data, v)
    

class TestErrorAtomFields(test.TestCase):
    
    def testNotRegistered(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.ModelNotRegistered,m.save)
        self.assertRaises(stdnet.ModelNotRegistered,m._meta.table)
    
    def testNotSaved(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.StdNetException,m.delete)    


