from hashlib import sha1
import json

from stdnet.lib import RedisScript, read_lua_file, get_script
from stdnet.lib.exceptions import NoScriptError

from .base import TestCase


to_charlist = lambda x: [x[c:c + 1] for c in range(len(x))]
binary_set = lambda x : set(to_charlist(x))


class test_script(RedisScript):
    script = (read_lua_file('utils/redis.lua'),
              '''\
js = cjson.decode(KEYS[1])
return cjson.encode(js)''')
    
    def callback(self, request, result, args, **options):
        return json.loads(result.decode(request.encoding))
    

class Receiver(object):
    
    def __init__(self):
        self.requests = []
        
    def __call__(self, sender, request = None, **kwargs):
        self.requests.append(request)
        
    def get(self):
        r = self.requests
        self.requests = []
        return r


class ScriptingCommandsTestCase(TestCase):
    tag = 'script'
    default_run = False

    def testEvalSimple(self):
        self.client = self.get_client()
        r = self.client.eval('return {1,2,3,4,5,6}')
        self.assertTrue(isinstance(r,list))
        self.assertEqual(len(r),6)
        
    def testTable(self):
        self.client = self.get_client()
        r = self.client.eval('return {name="mars", mass=0.11, orbit = 1.52}')
        self.assertTrue(isinstance(r,list))
        self.assertEqual(len(r), 0)
        
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
    
    def testScript(self):
        script = get_script('test_script')
        self.assertTrue(script.script)
        sha = sha1(script.script.encode('utf-8')).hexdigest()
        self.assertEqual(script.sha1,sha)
        
    def testEvalSha(self):
        self.assertEqual(self.client.script_flush(),True)
        re = Receiver()
        self.client.signal_on_received.connect(re)
        r = self.client.script_call('test_script',json.dumps([1,2,3,4,5,6]))
        self.assertEqual(r,[1,2,3,4,5,6])
        self.assertEqual(len(re.get()),2)
        r = self.client.script_call('test_script',json.dumps([1,2,3]))
        self.assertEqual(r,[1,2,3])
        self.assertEqual(len(re.get()),1)
        self.assertEqual(self.client.script_flush(),True)
        self.assertEqual(len(re.get()),1)
        r = self.client.script_call('test_script',json.dumps([1,2,3,4,5,'bla']))
        self.assertEqual(r,[1,2,3,4,5,'bla'])
        self.assertEqual(len(re.get()),2)
        
    def testEvalShaPipeline(self):
        self.assertEqual(self.client.script_flush(),True)
        pipe = self.client.pipeline()
        pipe.script_call('test_script',['ciao']).sadd('bla','foo')
        result = pipe.execute()
        self.assertTrue(isinstance(result[0],NoScriptError))
        self.assertTrue(result[1])

    def testEvalShaPipeline(self):
        self.assertEqual(self.client.script_flush(),True)
        pipe = self.client.pipeline()
        pipe.script_call('test_script',json.dumps({'foo':[1,2]}))\
            .sadd('bla','foo')
        result = pipe.execute(load_script = True)
        self.assertEqual(len(result),2)
        self.assertEqual(result[0],{'foo':[1,2]})
    