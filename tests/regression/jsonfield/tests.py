import os
import json
import time
from datetime import date, datetime
from decimal import Decimal
from random import random, randint

import stdnet
from stdnet import test
from stdnet.utils import zip, is_string, to_string, unichr, ispy3k
from stdnet.utils.populate import populate

from examples.models import Statistics, Statistics2, Statistics3


def make_random(size = 5, maxsize = 10, nesting = 1, level = 0):
    keys = populate(size = size)
    if level:
        keys.append('')
    level += 1
    for key in keys:
        if nesting:
            yield key,dict(make_random(size = randint(0,maxsize),
                                       maxsize = maxsize,
                                       nesting = nesting - 1,
                                       level = level))
        else:
            yield key,random()

    
class TestJsonField(test.TestModelBase):
    model = Statistics
        
    def testMetaData(self):
        field = Statistics._meta.dfields['data']
        self.assertEqual(field.sep,None)
        
    def testCreate(self):
        mean = Decimal('56.4')
        started = date(2010,1,1)
        timestamp = datetime.now()
        a = self.model(dt = date.today(), data = {'mean': mean,
                                                  'std': 5.78,
                                                  'started': started,
                                                  'timestamp':timestamp}).save()
        self.assertEqual(a.data['mean'],mean)
        a = self.model.objects.get(id = a.id)
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
        
    def testValueError(self):
        a = Statistics(dt = date.today(),
                       data = {'mean': self})
        self.assertFalse(a.is_valid())
        self.assertRaises(stdnet.FieldValueError,a.save)
        

class TestJsonFieldSep(test.TestModelBase):
    model = Statistics2
    
    def testMetaData(self):
        field = self.model._meta.dfields['data']
        self.assertEqual(field.sep,'__')
        
    def testCreate(self):
        mean = Decimal('56.4')
        started = date(2010,1,1)
        timestamp = datetime.now()
        a = self.model(dt = date.today(),
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
        

class TestJsonFieldAsData(test.TestModelBase):
    '''Test a model with a JSONField which expand as instance fields.
The `as_string` atttribute is set to ``False``.'''
    model = Statistics3
    def_data = {'mean': 1.0,
                'std': 5.78,
                'pv': 3.2,
                'name': 'bla',
                'dt': date.today()}
    
    def_baddata = {'': 3.2,
                 'ts': {'a':[1,2,3,4,5,6,7],
                        'b':[10,11,12]},
                 'mean': {'1y':1.0,'2y':1.1},
                 'std': {'1y':4.0,'2y':5.1},
                 'dt': datetime.now()}
    
    def_data2 = {'pv': {'':3.2,
                        'ts': {'a':[1,2,3,4,5,6,7],
                               'b':[10,11,12]},
                        'mean': {'1y':1.0,'2y':1.1},
                        'std': {'1y':4.0,'2y':5.1}},
                'dt': datetime.now()}
    
    def make(self, data = None):
        data = data or self.def_data
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
        
    def testmakeEmptyError(self):
        '''Here we test when we have a key which is empty.'''
        m = self.make(self.def_baddata)
        self.assertFalse(m.is_valid())
        self.assertRaises(stdnet.FieldValueError,m.save)
        
    def testmakeEmpty(self):
        m = self.make(self.def_data2)
        self.assertTrue(m.is_valid())
        cdata = m.cleaned_data
        self.assertEqual(len(cdata),9)
        self.assertFalse('data' in cdata)
        self.assertEqual(cdata['data__pv__mean__1y'],'1.0')
        obj = m.save()
        obj = self.model.objects.get(id = obj.id)
        self.assertEqual(obj.data['dt'].date(),date.today())
        self.assertEqual(obj.data__pv__mean__1y,1.0)
        self.assertEqual(obj.data__dt.date(),date.today())
        
    def testmakeEmpty2(self):
        m = self.make({'ts':[1,2,3,4]})
        obj = m.save()
        obj = self.model.objects.get(id = obj.id)
        self.assertEqual(obj.data,{'ts':[1,2,3,4]})
        
    def testFuzzy(self):
        data = dict(make_random(nesting = 3))
        m = self.make(data)
        self.assertTrue(m.is_valid())
        cdata = m.cleaned_data
        for k in cdata:
            if k is not 'name':
                self.assertTrue(k.startswith('data__'))
        obj = m.save()
        obj = self.model.objects.get(id = obj.id)
        self.assertEqualDict(data,obj.data)
    
    def assertEqualDict(self,data1,data2):
        for k in list(data1):
            v1 = data1.pop(k)
            v2 = data2.pop(k)
            if isinstance(v1,dict):
                self.assertEqualDict(v1,v2)
            else:
                self.assertAlmostEqual(v1,v2)
        self.assertFalse(data1)
        self.assertFalse(data2)
            
    