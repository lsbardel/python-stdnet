from stdnet import test, struct



class TestStruct(test.TestCase):
    
    def testList(self):
        l = struct.list()
        self.assertEqual(l.size(),0)