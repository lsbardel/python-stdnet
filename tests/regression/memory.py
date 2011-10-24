from stdnet import test
from stdnet.utils.structures import OrderedDict
from stdnet.utils import populate, zip


class TestOrderedDict(test.TestCase):
    '''This test is really only needed for python 2.6'''
    def setUp(self):
        self.keys = list(set(populate(datatype = 'date', size = 100)))
        self.vals = populate(datatype = 'string', size = len(self.keys))
        self.data = zip(self.keys,self.vals)
    
    def testOrdered(self):
        d = OrderedDict()
        for k,v in self.data:
            d[k] = v
        kp = None
        for k,k2 in zip(d,self.keys):
            self.assertEqual(k,k2)
        
