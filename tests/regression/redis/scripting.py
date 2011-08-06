    
from .base import *


to_charlist = lambda x: [x[c:c + 1] for c in range(len(x))]
binary_set = lambda x : set(to_charlist(x))


class ScriptingCommandsTestCase(BaseTest):
    tag         = 'script'
    default_run = False

    def setUp(self):
        self.client = self.get_client()
        self.client.flushdb()
        
    def tearDown(self):
        self.client.flushdb()
        
    def testEvalSimple(self):
        r = self.client.eval('return {1,2,3,4,5,6}')
        self.assertTrue(isinstance(r,list))
        self.assertEqual(len(r),6)