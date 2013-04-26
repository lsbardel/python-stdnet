import random

import stdnet
from stdnet.utils import test

from examples.models import SimpleModel


class TestManager(test.TestCase):
    model = SimpleModel
    
    @classmethod
    def after_setup(cls):
        cls.m = cls.mapper()
        names = test.populate('string', 100, min_len=6, max_len=20)
        with cls.m[cls.model].session().begin() as t:
            for name in names:
                t.add(SimpleModel(code=name))
        yield t.on_result
        
    def test_manager(self):
        m = self.m
        self.assertEqual(m[SimpleModel], m.simplemodel)
    
    def testGetOrCreate(self):
        objects = self.m[SimpleModel]
        v,  created = yield objects.get_or_create(code='test')
        self.assertTrue(created)
        self.assertEqual(v.code,'test')
        v2, created = yield objects.get_or_create(code='test')
        self.assertFalse(created)
        self.assertEqual(v,v2)
        
    def testGet(self):
        objects = self.m[SimpleModel]
        v, created = yield objects.get_or_create(code='test2')
        self.assertTrue(created)
        v1 = yield objects.get(code='test2')
        self.assertEqual(v1, v)
        
    def testGetError(self):
        '''Test for a ObjectNotFound exception.'''
        objects = self.m[SimpleModel]
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                lambda: objects.get(code='test3'))
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                lambda: objects.get(id=400))
        
    def testEmptyIDFilter(self):
        objects = self.m[SimpleModel]
        yield self.async.assertEqual(objects.filter(id=400).count(), 0)
        yield self.async.assertEqual(objects.filter(id=1).count(), 1)
        yield self.async.assertEqual(objects.filter(id=2).count(), 1)
        
    def testUniqueFilter(self):
        objects = self.m[SimpleModel]
        yield self.async.assertEqual(objects.filter(code='test4').count(), 0)
        yield objects.get_or_create(code='test4')
        yield self.async.assertEqual(objects.filter(code='test4').count(), 1)
        yield self.async.assertEqual(objects.filter(code='foo').count(), 0)
        
    def testIndexFilter(self):
        objects = self.m.simplemodel
        yield self.async.assertEqual(objects.filter(group='g1').count(), 0)
        v, created = yield objects.get_or_create(code='test5', group='g2')
        yield self.async.assertEqual(objects.filter(group='g1').count(), 0)
        yield self.async.assertEqual(objects.filter(group='g2').count(), 1)
        v1 = yield objects.get(group='g2')
        self.assertEqual(v, v1)
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                      objects.get, group='g1')
        v2, created = yield objects.get_or_create(code='test6', group='g2')
        yield self.async.assertEqual(objects.filter(group='g2').count(), 2)
        yield self.async.assertRaises(stdnet.QuerySetError,
                                      objects.get, group='g2')
        
    def testNoFilter(self):
        objects = self.m[SimpleModel]
        filter1 = lambda : objects.filter(description = 'bo').count()
        yield self.async.assertRaises(stdnet.QuerySetError, filter1)
        
    def testContainsAll(self):
        '''Test filter when performing a all request'''
        objects = self.m[SimpleModel]
        qs = objects.query()
        all = yield qs.all()
        self.assertTrue(all)
        self.assertTrue(qs.backend_query())
        self.assertTrue(1 in qs)
        be = qs.backend_query()
        self.assertEqual(be.cache[None], all)
        
        
