from .base import TestCase


to_charlist = lambda x: [x[c:c + 1] for c in range(len(x))]
binary_set = lambda x : set(to_charlist(x))


class ScriptingCommandsTestCase(TestCase):
    tag = 'script'
    default_run = False

    def testEvalSimple(self):
        self.client = self.get_client()
        r = self.client.eval('return {1,2,3,4,5,6}')
        self.assertTrue(isinstance(r,list))
        self.assertEqual(len(r),6)
        
    def testDelPattern(self):
        c = self.get_client()
        items = ('bla',1,
                 'bla1','ciao',
                 'bla2','foo',
                 'xxxx','moon',
                 'blaaaaaaaaaaaaaa','sun',
                 'xyyyy','earth')
        c.execute_command('MSET', *items)
        N = c.delpattern('bla*')
        self.assertEqual(N,4)
        self.assertFalse(c.exists('bla'))
        self.assertFalse(c.exists('bla1'))
        self.assertFalse(c.exists('bla2'))
        self.assertFalse(c.exists('blaaaaaaaaaaaaaa'))
        self.assertEqual(c.get('xxxx'),b'moon')
        N = c.delpattern('x*')
        self.assertEqual(N,2)
        