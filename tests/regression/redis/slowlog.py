from . import base

class SlowLogTestCase(base.TestCase):
    
    def setUp(self):
        super(SlowLogTestCase,self).setUp()
        self.slowlog = self.client.config_get('slowlog-log-slower-than')
        self.assertTrue(self.client.config_set('slowlog-log-slower-than', 0))
        self.assertTrue(self.client.slowlog_reset())
        
    def tearDown(self):
        self.assertTrue(self.client.config_set('slowlog-log-slower-than',
                               self.slowlog['slowlog-log-slower-than']))
        
    def testOneCommand(self):
        self.client.set('bla','foo')
        res = self.client.slowlog_get(2)
        self.assertTrue(res)
        self.assertEqual(len(res),2)
        self.assertEqual(res[0]['command'],'SET')
        self.assertTrue(res[0]['microseconds']>0)
        self.assertTrue(res[0]['timestamp']>0)
        self.assertEqual(res[0]['args'],(b'bla',b'foo'))
        self.assertTrue(res[0]['id']>res[1]['id'])
        
    def testReset(self):
        self.client.set('bla','foo')
        res = self.client.slowlog_reset()
        res = self.client.slowlog_get(100)
        self.assertTrue(res)
        self.assertEqual(len(res),1)
        self.assertEqual(res[0]['command'],'SLOWLOG')
        self.assertEqual(res[0]['args'],(b'RESET',))
        
        
class TestNestedLua(base.TestCase):
    
    def testNested(self):
        s = 'return {{name="luca",bla="foo"},{1,2,3,{{{}}}}}'
        r = self.client.eval(s,())