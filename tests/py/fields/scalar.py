'''Scalar fields such as Char, Integer, Float and Date, DateTime, Byte fields'''
import os
import json
import time
from datetime import date, datetime
from decimal import Decimal

import stdnet
from stdnet import FieldValueError
from stdnet.utils import test, populate, zip, is_string, to_string, unichr, ispy3k

from examples.models import TestDateModel, DateData,\
                             Page, SimpleModel, Environment, NumericData

NUM_DATES = 100
names = populate('string',NUM_DATES, min_len = 5, max_len = 20)
dates = populate('date', NUM_DATES, start=date(2010,5,1), end=date(2010,6,1))


class TestAtomFields(test.CleanTestCase):
    model = TestDateModel
    
    def setUp(self):
        self.register()
        
    def create(self):
        session = self.session()
        with session.begin():
            for na,dt in zip(names,dates):
                session.add(self.model(person=na, name=na, dt=dt))
        return session
            
    def testFilter(self):
        session = self.create()
        query = session.query(self.model)
        all = query.all()
        self.assertEqual(len(dates),len(all))
        N = 0
        done_dates = set()
        for dt in dates:
            if dt not in done_dates:
                done_dates.add(dt)
                elems = query.filter(dt=dt)
                N += elems.count()
                for elem in elems:
                    self.assertEqual(elem.dt,dt)
        self.assertEqual(len(all),N)
        
    def testDelete(self):
        self.create()
        N = 0
        done_dates = set()
        for dt in dates:
            if dt not in done_dates:
                done_dates.add(dt)
                objs = TestDateModel.objects.filter(dt=dt)
                N += objs.count()
                objs.delete()
        all = TestDateModel.objects.query()
        self.assertEqual(len(all),0)
        
        done_dates = set()
        for dt in dates:
            if dt not in done_dates:
                done_dates.add(dt)
                objs = TestDateModel.objects.filter(dt=dt)
                self.assertEqual(objs.count(),0)
                
        # The only key remaining is the ids key for the AutoField
        TestDateModel.objects.clean()
        keys = list(TestDateModel.objects.keys())
        self.assertEqual(len(keys),1)
        

class TestCharFields(test.CleanTestCase):
    model = SimpleModel

    def setUp(self):
        self.register()
            
    def testUnicode(self):
        unicode_string = unichr(500) + to_string('ciao') + unichr(300)
        self.model(code = unicode_string).save()
        m = self.model.objects.get(id = 1)
        self.assertEqual(m.code, unicode_string)
        if ispy3k:
            self.assertEqual(str(m),unicode_string)
        else:
            code = unicode_string.encode('utf-8')
            self.assertEqual(str(m),code)
        
    
class TestNumericData(test.CleanTestCase):
    model = NumericData

    def setUp(self):
        self.register()
            
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
        self.assertRaises(stdnet.FieldValueError,NumericData().save)
                
        
class TestIntegerField(test.CleanTestCase):
    model = Page

    def setUp(self):
        self.register()
            
    def testDefaultValue(self):
        p = Page()
        self.assertEqual(p.in_navigation,1)
        p = Page(in_navigation = '4')
        self.assertEqual(p.in_navigation,4)
        self.assertRaises(FieldValueError, p = Page, in_navigation = 'foo')
        p.save()
        self.assertEqual(p.in_navigation,4)
        p = Page.objects.get(id = p.id)
        self.assertEqual(p.in_navigation,4)
        
    def testNotValidated(self):
        p = Page().save()
        self.assertRaises(FieldValueError, Page, in_navigation = 'bla')
        
    def testZeroValue(self):
        p = Page(in_navigation = 0)
        self.assertEqual(p.in_navigation,0)
        p.save()
        self.assertEqual(p.in_navigation,0)
        p = Page.objects.get(id = p.id)
        self.assertEqual(p.in_navigation,0)
               

class TestDateData(test.CleanTestCase):
    model = DateData

    def setUp(self):
        self.register()
        
    def testDateindateTime(self):
        v = DateData(dt2 = date.today()).save()
        v = DateData.objects.get(id = v.id)
        self.assertEqual(v.dt1,None)
        self.assertEqual(v.dt2.date(),date.today())
        
    def testDefaultdate(self):
        v = DateData().save()
        self.assertEqual(v.dt1,None)
        self.assertEqual(v.dt2.date(),date.today())
        v.dt2 = None
        v.save()
        self.assertEqual(v.dt2.date(),date.today())
        

class TestBoolField(test.CleanTestCase):
    model = NumericData

    def setUp(self):
        self.register()
         
    def testMeta(self):
        self.assertEqual(len(self.model._meta.indices),1)
        index = self.model._meta.indices[0]
        self.assertEqual(index.type,'bool')
        self.assertEqual(index.index,True)
        self.assertEqual(index.name,index.attname)
        return index
        
    def testSerializeAndScoreFun(self):
        index = self.testMeta()
        for fname in ('scorefun','serialize'):
            func = getattr(index,fname)
            self.assertEqual(func(True),1)
            self.assertEqual(func(False),0)
            self.assertEqual(func(4),1)
            self.assertEqual(func(0),0)
            self.assertEqual(func('bla'),1)
            self.assertEqual(func(''),0)
            self.assertEqual(func(None),0)
        
    def test_bool_value(self):
        d = self.model(pv=1.).save()
        d = self.model.objects.get(id=d.id)
        self.assertEqual(d.ok,False)
        d.ok = 'jasxbhjaxsbjxsb'
        self.assertRaises(FieldValueError, d.save)
        d.ok = True
        d.save()
        d = self.model.objects.get(id = d.id)
        self.assertEqual(d.ok,True)
          
    
class TestByteField(test.CleanTestCase):
    model = SimpleModel

    def setUp(self):
        self.register()
        
    def testMetaData(self):
        field = SimpleModel._meta.dfields['somebytes']
        self.assertEqual(field.type,'bytes')
        self.assertEqual(field.internal_type,'bytes')
        self.assertEqual(field.index,False)
        self.assertEqual(field.name,field.attname)
        return field
        
    def testValue(self):
        v = SimpleModel(code='one', somebytes=to_string('hello'))
        self.assertEqual(v.somebytes, b'hello')
        v.save()
        v = SimpleModel.objects.get(code = 'one')
        self.assertEqual(v.somebytes,b'hello')
        
    def testValueByte(self):
        b = os.urandom(8)
        v = SimpleModel(code='one', somebytes=b)
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes, b)
        v.save()
        v = SimpleModel.objects.get(code = 'one')
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes,b)


class TestPickleObjectField(test.CleanTestCase):
    model = Environment
    
    def setUp(self):
        self.register()
        
    def testMetaData(self):
        field = self.model._meta.dfields['data']
        self.assertEqual(field.type,'object')
        self.assertEqual(field.internal_type,'bytes')
        self.assertEqual(field.index,False)
        self.assertEqual(field.name,field.attname)
        return field
    
    def testOkObject(self):
        v = self.model(data = ['ciao','pippo'])
        self.assertEqual(v.data, ['ciao','pippo'])
        v.save()
        self.assertEqual(v.data, ['ciao','pippo'])
        v = self.model.objects.get(id = v.id)
        self.assertEqual(v.data, ['ciao','pippo'])
        
    def testRecursive(self):
        '''Silly test to test both pickle field and pickable instace'''
        v = self.model(data = ('ciao','pippo', 4, {})).save()
        v2 = self.model(data = v)
        self.assertEqual(v2.data,v)
        v2.save()
        self.assertEqual(v2.data,v)
        v2 = self.model.objects.get(id = v2.id)
        self.assertEqual(v2.data, v)
    

class TestErrorAtomFields(test.CleanTestCase):

    def setUp(self):
        self.register()
  
    def testNotRegistered(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.ModelNotRegistered,m.save)
    
    def testNotSaved(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.StdNetException,m.delete)    


