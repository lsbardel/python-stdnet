from stdnet import getcache
from stdnet.utils import test

class TestCache(test.TestCase):
    
    def testSimple(self):
        c = getcache()
        self.assertTrue(c)