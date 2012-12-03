from stdnet import getcache
from stdnet.utils import test

class TestCache(test.TestCase):
    multipledb = 'redis'
    
    def testSimple(self):
        c = getcache(self.connection_string)
        self.assertTrue(c)
        
        
class TestNoCache(test.TestCase):
    multipledb = False
    
    def testSimple(self):
        self.assertRaises(NotImplementedError, getcache, 'mongo://')