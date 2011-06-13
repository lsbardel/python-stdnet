import random

import stdnet
from stdnet import test
from examples.models import SimpleModel
from stdnet.utils import populate

LEN = 100
names = populate('string',LEN, min_len = 5, max_len = 20)


class TestTransactions(test.TestCase):
    
    def setUp(self):
        self.orm.register(SimpleModel)
    
    def unregister(self):
        self.orm.unregister(SimpleModel)
        
    def testSave(self):
        with SimpleModel.transaction() as t:
            self.assertEqual(t.server,SimpleModel._meta.cursor)
            s = SimpleModel(code = 'test', description = 'just a test').save(transaction = t)
            self.assertTrue(s.id)
            self.assertRaises(SimpleModel.DoesNotExist,SimpleModel.objects.get,id=s.id)
            s2 = SimpleModel(code = 'test2', description = 'just a test').save(transaction = t)

        all = list(SimpleModel.objects.all())
        self.assertEqual(len(all),2)
        v = SimpleModel.objects.get(code = 'test')
        self.assertEqual(v.description,'just a test')
        
    def testDelete(self):
        s = SimpleModel(code = 'test', description = 'just a test').save()
        self.assertEqual(SimpleModel.objects.get(id = s.id),s)
        s.delete()
        self.assertRaises(SimpleModel.DoesNotExist,SimpleModel.objects.get,id=s.id)
    
        