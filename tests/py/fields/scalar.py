'''Scalar fields such as Char, Integer, Float and Date, DateTime, Byte fields'''
import os
import json
import time
from datetime import date, datetime
from decimal import Decimal

from pulsar import multi_async
from pulsar.apps.test import sequential

import stdnet
from stdnet import FieldValueError
from stdnet.utils import test, populate, zip, is_string, to_string, unichr, ispy3k

from examples.models import TestDateModel, DateData,\
                             Page, SimpleModel, Environment, NumericData

NUM_DATES = 100
names = populate('string',NUM_DATES, min_len = 5, max_len = 20)
dates = populate('date', NUM_DATES, start=date(2010,5,1), end=date(2010,6,1))


class TestDateModel2(TestDateModel):
    pass

@sequential
class TestAtomFields(test.TestCase):
    model = TestDateModel
    
    @classmethod
    def after_setup(cls):
        cls.register()
        
    def tearDown(self):
        return self.clear_all()
        
    def create(self):
        session = self.session()
        with session.begin() as t:
            for na,dt in zip(names, dates):
                t.add(self.model(person=na, name=na, dt=dt))
        yield t.on_result
        yield session
            
    def testFilter(self):
        session = yield self.create()
        query = session.query(self.model)
        all = yield query.all()
        self.assertEqual(len(dates), len(all))
        N = 0
        done_dates = {}
        for dt in dates:
            if dt not in done_dates:
                done_dates[dt] = query.filter(dt=dt).all()
        done_dates = yield multi_async(done_dates)
        N = 0
        for dt, elems in done_dates.items():
            N += len(elems)
            for elem in elems:
                self.assertEqual(elem.dt, dt)
        self.assertEqual(len(all), N)
        
    def testDelete(self):
        self.create()
        N = 0
        done_dates = set()
        for dt in dates:
            if dt not in done_dates:
                done_dates.add(dt)
                objs = TestDateModel.objects.filter(dt=dt)
                N += yield objs.count()
                yield objs.delete()
        all = TestDateModel.objects.query()
        self.assertEqual(len(all),0)
        
        done_dates = set()
        for dt in dates:
            if dt not in done_dates:
                done_dates.add(dt)
                objs = TestDateModel.objects.filter(dt=dt)
                self.assertEqual(objs.count(),0)
                
        # The only key remaining is the ids key for the AutoIdField
        TestDateModel.objects.clean()
        keys = list(TestDateModel.objects.keys())
        self.assertEqual(len(keys),1)
        

class TestCharFields(test.TestCase):
    model = SimpleModel

    @classmethod
    def after_setup(cls):
        cls.register()
            
    def testUnicode(self):
        unicode_string=unichr(500) + to_string('ciao') + unichr(300)
        m = yield self.model(code=unicode_string).save()
        m = yield self.model.objects.get(id=m.id)
        self.assertEqual(m.code, unicode_string)
        if ispy3k:
            self.assertEqual(str(m), unicode_string)
        else:
            code = unicode_string.encode('utf-8')
            self.assertEqual(str(m), code)
        
    
class TestNumericData(test.TestCase):
    model = NumericData

    @classmethod
    def after_setup(cls):
        cls.register()
            
    def testDefaultValue(self):
        d = yield NumericData(pv = 1.).save()
        self.assertAlmostEqual(d.pv, 1.)
        self.assertAlmostEqual(d.vega, 0.)
        self.assertAlmostEqual(d.delta, 1.)
        self.assertEqual(d.gamma, None)
        
    def testDefaultValue2(self):
        d = yield NumericData(pv=0., delta=0.).save()
        self.assertAlmostEqual(d.pv, 0.)
        self.assertAlmostEqual(d.vega, 0.)
        self.assertAlmostEqual(d.delta, 0.)
        self.assertEqual(d.gamma, None)
        
    def testFieldError(self):
        yield self.async.assertRaises(stdnet.FieldValueError, NumericData().save)
                
        
class TestIntegerField(test.TestCase):
    model = Page

    @classmethod
    def after_setup(cls):
        cls.register()
            
    def testDefaultValue(self):
        p = Page()
        self.assertEqual(p.in_navigation, 1)
        p = Page(in_navigation='4')
        self.assertEqual(p.in_navigation, 4)
        self.assertRaises(FieldValueError, p=Page, in_navigation='foo')
        yield p.save()
        self.assertEqual(p.in_navigation, 4)
        p = yield Page.objects.get(id=p.id)
        self.assertEqual(p.in_navigation, 4)
        
    def testNotValidated(self):
        p = yield Page().save()
        self.assertRaises(FieldValueError, Page, in_navigation='bla')
        
    def testZeroValue(self):
        p = Page(in_navigation=0)
        self.assertEqual(p.in_navigation, 0)
        yield p.save()
        self.assertEqual(p.in_navigation, 0)
        p = yield Page.objects.get(id=p.id)
        self.assertEqual(p.in_navigation, 0)
               

class TestDateData(test.TestCase):
    model = DateData

    @classmethod
    def after_setup(cls):
        cls.register()
        
    def testDateindateTime(self):
        v = yield DateData(dt2 = date.today()).save()
        v = yield DateData.objects.get(id=v.id)
        self.assertEqual(v.dt1, None)
        self.assertEqual(v.dt2.date(), date.today())
        
    def testDefaultdate(self):
        v = yield DateData().save()
        self.assertEqual(v.dt1, None)
        self.assertEqual(v.dt2.date(), date.today())
        v.dt2 = None
        yield v.save()
        self.assertEqual(v.dt2.date(), date.today())
        

class TestBoolField(test.TestCase):
    model = NumericData

    @classmethod
    def after_setup(cls):
        cls.register()
         
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
        d = yield self.model(pv=1.).save()
        d = yield self.model.objects.get(id=d.id)
        self.assertEqual(d.ok, False)
        d.ok = 'jasxbhjaxsbjxsb'
        self.assertRaises(FieldValueError, d.save)
        d.ok = True
        yield d.save()
        d = yield self.model.objects.get(id=d.id)
        self.assertEqual(d.ok, True)
          
    
class TestByteField(test.TestCase):
    model = SimpleModel

    @classmethod
    def after_setup(cls):
        cls.register()
        
    def testMetaData(self):
        field = SimpleModel._meta.dfields['somebytes']
        self.assertEqual(field.type,'bytes')
        self.assertEqual(field.internal_type,'bytes')
        self.assertEqual(field.index,False)
        self.assertEqual(field.name,field.attname)
        return field
        
    def testValue(self):
        v = SimpleModel(code='cgfgcgf', somebytes=to_string('hello'))
        self.assertEqual(v.somebytes, b'hello')
        yield v.save()
        v = yield SimpleModel.objects.get(id=v.id)
        self.assertEqual(v.somebytes, b'hello')
        
    def testValueByte(self):
        b = os.urandom(8)
        v = SimpleModel(code='sdcscdsc', somebytes=b)
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes, b)
        yield v.save()
        v = yield SimpleModel.objects.get(id=v.id)
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes, b)


class TestPickleObjectField(test.TestCase):
    model = Environment
    
    @classmethod
    def after_setup(cls):
        cls.register()
        
    def testMetaData(self):
        field = self.model._meta.dfields['data']
        self.assertEqual(field.type,'object')
        self.assertEqual(field.internal_type,'bytes')
        self.assertEqual(field.index,False)
        self.assertEqual(field.name,field.attname)
        return field
    
    def testOkObject(self):
        v = self.model(data=['ciao','pippo'])
        self.assertEqual(v.data, ['ciao','pippo'])
        yield v.save()
        self.assertEqual(v.data, ['ciao','pippo'])
        v = yield self.model.objects.get(id=v.id)
        self.assertEqual(v.data, ['ciao','pippo'])
        
    def testRecursive(self):
        '''Silly test to test both pickle field and pickable instace'''
        v = yield self.model(data=('ciao','pippo', 4, {})).save()
        v2 = self.model(data=v)
        self.assertEqual(v2.data, v)
        yield v2.save()
        self.assertEqual(v2.data, v)
        v2 = yield self.model.objects.get(id=v2.id)
        self.assertEqual(v2.data, v)
    

class TestErrorAtomFields(test.TestCase):

    def testNotRegistered(self):
        m = TestDateModel2(name=names[1], dt=dates[0], person='sdcbsc')
        self.assertRaises(stdnet.ModelNotRegistered, m.save)
    
    def testNotSaved(self):
        m = TestDateModel2(name=names[1], dt=dates[0])
        self.assertRaises(stdnet.StdNetException, m.delete)    


