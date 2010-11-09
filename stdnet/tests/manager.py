import random
from itertools import izip

import stdnet
from stdnet.test import TestCase
from examples.models import SimpleModel
    

class TestManager(TestCase):

    def setUp(self):
        self.orm.register(SimpleModel)
    
    def unregister(self):
        self.orm.unregister(SimpleModel)
        
    def testGetOrCreate(self):
        v,created = SimpleModel.objects.get_or_create(code = 'test')
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
        '''Test for a ObjectNotFund exception.'''
        get1 = lambda : SimpleModel.objects.get(code = 'test2')
        get2 = lambda : SimpleModel.objects.get(id = 34)
        self.assertRaises(stdnet.ObjectNotFund,get1)
        self.assertRaises(stdnet.ObjectNotFund,get2)