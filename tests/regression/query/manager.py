import random

import stdnet
from stdnet import test
from stdnet.utils import populate

from examples.models import SimpleModel

LEN = 100
names = populate('string',LEN, min_len = 5, max_len = 20)


class TestManager(test.TestCase):
    model = SimpleModel
    
    def setUp(self):
        self.register()
        
    def fill(self):
        with SimpleModel.objects.session().begin() as t:
            for name in names:
                t.session.add(SimpleModel(code = name))
                
    def testGetOrCreate(self):
        v, created = SimpleModel.objects.get_or_create(code = 'test')
        self.assertTrue(created)
        self.assertEqual(v.code,'test')
        v2,created = SimpleModel.objects.get_or_create(code = 'test')
        self.assertFalse(created)
        self.assertEqual(v,v2)
        
    def testGet(self):
        v,created = SimpleModel.objects.get_or_create(code = 'test')
        self.assertTrue(created)
        v1 = SimpleModel.objects.get(code = 'test')
        self.assertEqual(v1,v)
        
    def testGetError(self):
        '''Test for a ObjectNotFound exception.'''
        self.assertRaises(SimpleModel.DoesNotExist,
                          SimpleModel.objects.get, code = 'test2')
        self.assertRaises(SimpleModel.DoesNotExist,
                          SimpleModel.objects.get, id = 34)
        
    def testEmptyIDFilter(self):
        self.assertEqual(SimpleModel.objects.filter(id = 1).count(),0)
        SimpleModel.objects.get_or_create(code = 'test')
        self.assertEqual(SimpleModel.objects.filter(id = 1).count(),1)
        self.assertEqual(SimpleModel.objects.filter(id = 2).count(),0)
        
    def testUniqueFilter(self):
        self.assertEqual(SimpleModel.objects.filter(code = 'test').count(),0)
        SimpleModel.objects.get_or_create(code = 'test')
        self.assertEqual(SimpleModel.objects.filter(code = 'test').count(),1)
        self.assertEqual(SimpleModel.objects.filter(code = 'test2').count(),0)
        
    def testIndexFilter(self):
        self.assertEqual(SimpleModel.objects.filter(group = 'g1').count(),0)
        v,created=SimpleModel.objects.get_or_create(code = 'test',group = 'g2')
        self.assertEqual(SimpleModel.objects.filter(group = 'g1').count(),0)
        self.assertEqual(SimpleModel.objects.filter(group = 'g2').count(),1)
        v1 = SimpleModel.objects.get(group = 'g2')
        self.assertEqual(v,v1)
        get1 = lambda : SimpleModel.objects.get(group = 'g1')
        self.assertRaises(stdnet.ObjectNotFound,get1)
        v2,created =SimpleModel.objects.get_or_create(code = 'test2', group = 'g2')
        self.assertEqual(SimpleModel.objects.filter(group = 'g2').count(),2)
        get2 = lambda : SimpleModel.objects.get(group = 'g2')
        self.assertRaises(stdnet.QuerySetError,get2)
        
    def testNoFilter(self):
        filter1 = lambda : SimpleModel.objects.filter(description = 'bo').count()
        self.assertRaises(stdnet.QuerySetError,filter1)
        
    def testContainsAll(self):
        '''Test filter when performing a all request'''
        self.fill()
        qs = SimpleModel.objects.query()
        self.assertFalse('ciao' in qs)
        self.assertTrue(qs.backend_query())
        self.assertTrue(1 in qs)
        self.assertEqual(qs.cache(),{})
        
        
