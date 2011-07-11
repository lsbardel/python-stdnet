from stdnet import test, getdb
from stdnet.utils import zip
from stdnet.utils.populate import populate

from examples.models import SimpleModel


groups = lambda N : populate('choice',N,
                             choice_from=['football','rugby','swimming',\
                                          'running','cycling'])

class FuzzyTest(test.TestCase):
    
    def register(self):
        self.orm.register(SimpleModel)
        self.rpy = getdb().redispy
        
    def fuzzydata(self, size, min_len, max_len):
        g = groups(size)
        k = populate('string',size,min_len=10,max_len=20)
        d1 = populate('string',size,min_len=min_len,max_len=max_len)
        d2 = populate('string',size,min_len=min_len,max_len=max_len)
        return k,g,d1,d2
    
    def testSetBig(self):
        data = self.fuzzydata(1000,10000,100000)
        with SimpleModel.transaction() as t:
            for c,g,d1,d2 in zip(*data):
                SimpleModel(code = c, group = g,
                            description = d1, somebytes = d2).save(t)
        all = SimpleModel.objects.all()
        self.assertTrue(all.count())
        all =list(all)
        
            
            
        