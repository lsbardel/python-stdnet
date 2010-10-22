import random
from itertools import izip

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