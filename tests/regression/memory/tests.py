from stdnet.test import TestCase
from stdnet.utils.collections import OrderedDict
from stdnet.utils import populate, zip


class TestOrderedDict(TestCase):
    
    def setUp(self):
        self.keys = populate(datatype = 'date', size = 100)
        self.vals = populate(datatype = 'string', size = 100)
        self.data = zip(self.keys,self.vals)
    
    def _testOrdered(self):
        d = OrderedDict()
        for k,v in self.data:
            d[k] = v
        kp = None
        for k,k2 in zip(d,self.keys):
            self.assertEqual(k,k2)
        
