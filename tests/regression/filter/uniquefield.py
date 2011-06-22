from random import randint

from stdnet import test
from stdnet.utils import populate, zip, range

from examples.models import SimpleModel


SIZE = 200
sports = ['football','rugby','swimming','running','cycling']

codes = set(populate('string',SIZE, min_len = 5, max_len = 20))
SIZE = len(codes)
groups = populate('choice',SIZE,choice_from=sports)
codes = list(codes)

class TestUniqueFilter(test.TestCase):
    
    def setUp(self):
        self.orm.register(SimpleModel)
        with SimpleModel.transaction() as t:
            for n,g in zip(codes,groups):
                SimpleModel(code = n, group = g).save(t)
    
    def testFilterSimple(self):
        for i in range(10):
            i = randint(0,len(codes)-1)
            code = codes[i]
            qs = SimpleModel.objects.filter(code = code)
            self.assertEqual(qs.count(),1)
            self.assertTrue(qs.simple)
            self.assertEqual(qs[0].code,code)
            
    def testExcludeSimple(self):
        for i in range(10):
            i = randint(0,len(codes)-1)
            code = codes[i]
            r = SimpleModel.objects.exclude(code = code)
            self.assertEqual(r.count(),SIZE-1)
            self.assertFalse(code in set((o.code for o in r)))

