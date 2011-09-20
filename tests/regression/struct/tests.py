from stdnet import test, struct



class TestStruct(test.TestCase):
    
    def testList(self):
        l = struct.list()
        self.assertEqual(l.size(),0)
        l.push_back(3)
        l.push_back('save')
        self.assertEqual(l.size(),0)
        l.save()
        self.assertEqual(l.size(),2)
        self.assertEqual(list(l),['3','save'])
        
    def testSet(self):
        l = struct.set()
        self.assertEqual(l.size(),0)
        
    def testHash(self):
        l = struct.hash()
        self.assertEqual(l.size(),0)
        
    def testZset(self):
        l = struct.zset()
        self.assertEqual(l.size(),0)