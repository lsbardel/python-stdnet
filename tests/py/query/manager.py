import random

import stdnet
from stdnet.utils import test, populate

from examples.models import SimpleModel


class TestManager(test.TestCase):
    model = SimpleModel
    
    @classmethod
    def after_setup(cls):
        cls.register()
        names = populate('string', 100, min_len=6, max_len=20)
        with cls.model.objects.session().begin() as t:
            for name in names:
                t.add(SimpleModel(code=name))
        yield t.on_result
    
    def testGetOrCreate(self):
        v,  created = yield SimpleModel.objects.get_or_create(code='test')
        self.assertTrue(created)
        self.assertEqual(v.code,'test')
        v2, created = yield SimpleModel.objects.get_or_create(code='test')
        self.assertFalse(created)
        self.assertEqual(v,v2)
        
    def testGet(self):
        v, created = yield SimpleModel.objects.get_or_create(code='test2')
        self.assertTrue(created)
        v1 = yield SimpleModel.objects.get(code='test2')
        self.assertEqual(v1, v)
        
    def testGetError(self):
        '''Test for a ObjectNotFound exception.'''
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                lambda: SimpleModel.objects.get(code='test3'))
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                lambda: SimpleModel.objects.get(id=400))
        
    def testEmptyIDFilter(self):
        yield self.async.assertEqual(SimpleModel.objects.filter(id=400).count(), 0)
        yield self.async.assertEqual(SimpleModel.objects.filter(id=1).count(), 1)
        yield self.async.assertEqual(SimpleModel.objects.filter(id=2).count(), 1)
        
    def testUniqueFilter(self):
        yield self.async.assertEqual(SimpleModel.objects.filter(code='test4').count(), 0)
        yield SimpleModel.objects.get_or_create(code='test4')
        yield self.async.assertEqual(SimpleModel.objects.filter(code='test4').count(), 1)
        yield self.async.assertEqual(SimpleModel.objects.filter(code='foo').count(), 0)
        
    def testIndexFilter(self):
        yield self.async.assertEqual(SimpleModel.objects.filter(group='g1').count(), 0)
        v, created = yield SimpleModel.objects.get_or_create(code='test5', group='g2')
        yield self.async.assertEqual(SimpleModel.objects.filter(group='g1').count(), 0)
        yield self.async.assertEqual(SimpleModel.objects.filter(group='g2').count(), 1)
        v1 = yield SimpleModel.objects.get(group='g2')
        self.assertEqual(v, v1)
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                SimpleModel.objects.get, group='g1')
        v2, created = yield SimpleModel.objects.get_or_create(code='test6', group='g2')
        yield self.async.assertEqual(SimpleModel.objects.filter(group='g2').count(), 2)
        yield self.async.assertRaises(stdnet.QuerySetError,
                                SimpleModel.objects.get, group='g2')
        
    def testNoFilter(self):
        filter1 = lambda : SimpleModel.objects.filter(description = 'bo').count()
        self.assertRaises(stdnet.QuerySetError,filter1)
        
    def testContainsAll(self):
        '''Test filter when performing a all request'''
        qs = SimpleModel.objects.query()
        all = yield qs.all()
        self.assertTrue(all)
        self.assertTrue(qs.backend_query())
        self.assertTrue(1 in qs)
        self.assertEqual(qs.cache()[None], all)
        
        
