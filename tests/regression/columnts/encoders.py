from stdnet import test
from stdnet.apps.columnts import ColumnTS, nil, nan, ValueEncoder, DoubleEncoder


class TestEncoders(test.TestCase):
    
    def ts(self):
        return ColumnTS(value_pickler = ValueEncoder())
    
    def testMeta(self):
        ts = self.ts()
        self.assertTrue(isinstance(ts.value_pickler, ValueEncoder))
        #self.assertEqual(ts.value_pickler.dumps('ciao'),nil)
        self.assertEqual(ts.value_pickler.dumps(None), nil)
        v = ts.value_pickler.loads(nil)
        self.assertNotEqual(v,v)
        
    def testInt(self):
        e = ValueEncoder()
        self.assertEqual(e.value_length,9)
        b = e.dumps(3)
        self.assertEqual(len(b),9)
        self.assertEqual(e.loads(b),3)
        
    def testDouble(self):
        e = ValueEncoder()
        b = e.dumps(3.5)
        self.assertEqual(len(b),9)
        self.assertEqual(e.loads(b),3.5)
        v = e.dumps(nan)
        self.assertEqual(v,nil)
        r = e.loads(v)
        self.assertNotEqual(r,r)
        
    def testShortString(self):
        e = ValueEncoder()
        b = e.dumps('ciao')
        self.assertEqual(len(b),9)
        self.assertEqual(e.loads(b), 'ciao')
        b = e.dumps('ciaoxxx')
        self.assertEqual(len(b),9)
        self.assertEqual(e.loads(b), 'ciaoxxx')
        b = e.dumps('')
        self.assertEqual(len(b),9)
        self.assertEqual(e.loads(b), '')
        
    def testString(self):
        e = ValueEncoder()
        t = 'this is string has more than 7 characters'
        N = len(t)
        b = e.dumps(t)
        self.assertEqual(len(b),9+N)
        self.assertEqual(e.loads(b[0:1]+b[9:]), t)
        
    def testDoubleEncoder(self):
        e = DoubleEncoder()
        v = e.dumps(None)
        self.assertEqual(v,nil)
        v = e.dumps('bla')
        self.assertEqual(v,nil)
        v = e.dumps(nan)
        self.assertEqual(v,nil)
        v = e.dumps(56)
        self.assertNotEqual(v,nil)
        self.assertEqual(e.loads(v),56)
        self.assertEqual(e.loads(e.dumps(-7.8)),-7.8)
        self.assertNotEqual(e.loads(nil),e.loads(nil))
        self.assertNotEqual(e.loads('sdjkcbsdc'),e.loads('sdjkcbsdc'))
        
     

