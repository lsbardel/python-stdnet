import datetime
import unittest
import logging
from itertools import izip

import stdnet
from stdnet.test import TestCase
from stdnet.utils import populate

from examples.models import TestDateModel

NUM_DATES = 100
names = populate('string',NUM_DATES, min_len = 5, max_len = 20)
dates = populate('date', NUM_DATES, start=datetime.date(2010,5,1), end=datetime.date(2010,6,1))



class TestAtomFields(TestCase):
    
    def setUp(self):
        self.orm.register(TestDateModel)
        self.meta = TestDateModel._meta
    
    def unregister(self):
        self.orm.unregister(TestDateModel)
        
    def create(self):
        for na,dt in izip(names,dates):
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
        

class TestErrorAtomFields(TestCase):
    
    def testNotRegistered(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.ModelNotRegistered,m.save)
        self.assertRaises(stdnet.ModelNotRegistered,m._meta.table)
    
    def testNotSaved(self):
        m = TestDateModel(name = names[1], dt = dates[0])
        self.assertRaises(stdnet.StdNetException,m.delete)    
            