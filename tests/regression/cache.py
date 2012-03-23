from stdnet import test, getcache

class TestCache(test.TestCase):
    
    def testSimple(self):
        c = getcache()
        self.assertTrue(c)