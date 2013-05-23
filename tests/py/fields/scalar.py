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


class TestDateModel2(TestDateModel):
    pass


class TestAtomFields(test.TestWrite):
    model = TestDateModel
        
    def setUp(self):
        return self.create()
        
    def create(self):
        models = self.mapper
        with models.session().begin() as t:
            for na, dt in zip(names, dates):
                t.add(self.model(person=na, name=na, dt=dt))
        return t.on_result
            
    def testFilter(self):
        query = self.query()
        all = yield query.all()
        self.assertEqual(len(dates), len(all))
        N = 0
        done_dates = {}
        for dt in dates:
            if dt not in done_dates:
                done_dates[dt] = query.filter(dt=dt).all()
        done_dates = yield self.multi_async(done_dates)
        N = 0
        for dt, elems in done_dates.items():
            N += len(elems)
            for elem in elems:
                self.assertEqual(elem.dt, dt)
        self.assertEqual(len(all), N)
        
    def test_delete(self):
        N = 0
        query = self.query()
        done_dates = {}
        for dt in dates:
            if dt not in done_dates:
                done_dates[dt] = query.filter(dt=dt).count()
        done_dates = yield self.multi_async(done_dates)
        N = yield query.count()
        for d in done_dates.values():
            N -= d
        self.assertFalse(N)
        yield query.delete()
        query = self.query()
        all = yield query.all()
        self.assertFalse(all)
        done_dates = {}
        for dt in dates:
            if dt not in done_dates:
                done_dates[dt] = query.filter(dt=dt).count()
        done_dates = yield self.multi_async(done_dates)
        for d in done_dates.values():
            self.assertEqual(d, 0)
            
        # The only key remaining is the ids key for the AutoIdField
        session = self.session()
        yield session.clean(self.model)
        keys = session.keys(self.model)
        self.assertEqual(len(keys), 1)
        

class TestCharFields(test.TestCase):
    model = SimpleModel
            
    def testUnicode(self):
        models = self.mapper
        unicode_string=unichr(500) + to_string('ciao') + unichr(300)
        m = yield models.simplemodel.new(code=unicode_string)
        m = yield models.simplemodel.get(id=m.id)
        self.assertEqual(m.code, unicode_string)
        if ispy3k:
            self.assertEqual(str(m), unicode_string)
        else:
            code = unicode_string.encode('utf-8')
            self.assertEqual(str(m), code)
        
    
class TestNumericData(test.TestCase):
    model = NumericData

    def testDefaultValue(self):
        models = self.mapper
        d = yield models.numericdata.new(pv=1.)
        self.assertAlmostEqual(d.pv, 1.)
        self.assertAlmostEqual(d.vega, 0.)
        self.assertAlmostEqual(d.delta, 1.)
        self.assertEqual(d.gamma, None)
        
    def testDefaultValue2(self):
        models = self.mapper
        d = yield models.numericdata.new(pv=0., delta=0.)
        self.assertAlmostEqual(d.pv, 0.)
        self.assertAlmostEqual(d.vega, 0.)
        self.assertAlmostEqual(d.delta, 0.)
        self.assertEqual(d.gamma, None)
        
    def testFieldError(self):
        models = self.mapper
        yield self.async.assertRaises(stdnet.FieldValueError,
                                      models.numericdata.new)
                
        
class TestIntegerField(test.TestCase):
    model = Page
            
    def testDefaultValue(self):
        models = self.mapper
        p = Page()
        self.assertEqual(p.in_navigation, 1)
        p = Page(in_navigation='4')
        self.assertEqual(p.in_navigation, 4)
        self.assertRaises(FieldValueError, p=Page, in_navigation='foo')
        yield self.session().add(p)
        self.assertEqual(p.in_navigation, 4)
        p = yield models.page.get(id=p.id)
        self.assertEqual(p.in_navigation, 4)
        
    def testNotValidated(self):
        models = self.mapper
        p = yield models.page.new()
        self.assertRaises(FieldValueError, Page, in_navigation='bla')
        
    def testZeroValue(self):
        models = self.mapper
        p = models.page(in_navigation=0)
        self.assertEqual(p.in_navigation, 0)
        yield models.session().add(p)
        self.assertEqual(p.in_navigation, 0)
        p = yield models.page.get(id=p.id)
        self.assertEqual(p.in_navigation, 0)
               

class TestDateData(test.TestCase):
    model = DateData
        
    def testDateindateTime(self):
        models = self.mapper
        v = yield models.datedata.new(dt2=date.today())
        v = yield models.datedata.get(id=v.id)
        self.assertEqual(v.dt1, None)
        self.assertEqual(v.dt2.date(), date.today())
        
    def testDefaultdate(self):
        models = self.mapper
        v = yield models.datedata.new()
        self.assertEqual(v.dt1, None)
        self.assertEqual(v.dt2.date(), date.today())
        v.dt2 = None
        yield models.session().add(v)
        self.assertEqual(v.dt2.date(), date.today())
        

class TestBoolField(test.TestCase):
    model = NumericData
         
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
        models = self.mapper
        session = models.session()
        d = yield session.add(models.numericdata(pv=1.))
        d = yield models.numericdata.get(id=d.id)
        self.assertEqual(d.ok, False)
        d.ok = 'jasxbhjaxsbjxsb'
        self.assertRaises(FieldValueError, session.add, d)
        d.ok = True
        yield session.add(d)
        d = yield models.numericdata.get(id=d.id)
        self.assertEqual(d.ok, True)
          
    
class TestByteField(test.TestCase):
    model = SimpleModel
        
    def testMetaData(self):
        field = SimpleModel._meta.dfields['somebytes']
        self.assertEqual(field.type,'bytes')
        self.assertEqual(field.internal_type,'bytes')
        self.assertEqual(field.index,False)
        self.assertEqual(field.name,field.attname)
        return field
        
    def testValue(self):
        models = self.mapper
        v = models.simplemodel(code='cgfgcgf', somebytes=to_string('hello'))
        self.assertEqual(v.somebytes, b'hello')
        self.assertFalse(v.id)
        yield models.session().add(v)
        v = yield models.simplemodel.get(id=v.id)
        self.assertEqual(v.somebytes, b'hello')
        
    def testValueByte(self):
        models = self.mapper
        b = os.urandom(8)
        v = SimpleModel(code='sdcscdsc', somebytes=b)
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes, b)
        yield models.session().add(v)
        v = yield models.simplemodel.get(id=v.id)
        self.assertFalse(is_string(v.somebytes))
        self.assertEqual(v.somebytes, b)


class TestPickleObjectField(test.TestCase):
    model = Environment
    
    def testMetaData(self):
        field = self.model._meta.dfields['data']
        self.assertEqual(field.type,'object')
        self.assertEqual(field.internal_type,'bytes')
        self.assertEqual(field.index,False)
        self.assertEqual(field.name,field.attname)
        return field
    
    def testOkObject(self):
        session = self.session()
        v = self.model(data=['ciao','pippo'])
        self.assertEqual(v.data, ['ciao','pippo'])
        yield session.add(v)
        self.assertEqual(v.data, ['ciao','pippo'])
        v = yield session.query(self.model).get(id=v.id)
        self.assertEqual(v.data, ['ciao','pippo'])
        
    def testRecursive(self):
        '''Silly test to test both pickle field and pickable instance'''
        session = self.session()
        v = yield session.add(self.model(data=('ciao','pippo', 4, {})))
        v2 = self.model(data=v)
        self.assertEqual(v2.data, v)
        yield session.add(v2)
        self.assertEqual(v2.data, v)
        v2 = yield session.query(self.model).get(id=v2.id)
        self.assertEqual(v2.data, v)
    

class TestErrorAtomFields(test.TestCase):
    
    def testSessionNotAvailable(self):
        session = self.session()
        m = TestDateModel2(name=names[1], dt=dates[0], person='sdcbsc')
        self.assertRaises(stdnet.InvalidTransaction, session.add, m)
    
    def testNotSaved(self):
        session = self.session()
        m = TestDateModel2(name=names[1], dt=dates[0])
        self.assertRaises(stdnet.StdNetException, session.delete, m)    


