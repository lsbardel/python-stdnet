from hashlib import sha1
import json
import struct

from stdnet.conf import settings
from stdnet.lib import redis

from . import client


to_charlist = lambda x: [x[c:c + 1] for c in range(len(x))]
binary_set = lambda x : set(to_charlist(x))


class test_script(redis.RedisScript):
    script = (redis.read_lua_file('commands.utils'),
              '''\
local js = cjson.decode(ARGV[1])
return cjson.encode(js)''')
    
    def callback(self, request, result, args, **options):
        return json.loads(result.decode(request.encoding))
    

class Receiver(object):
    
    def __init__(self):
        self.requests = []
        
    def __call__(self, sender, request, **kwargs):
        self.requests.append(request)
        
    def get(self):
        r = self.requests
        self.requests = []
        return r


class ScriptingCommandsTestCase(client.TestCase):

    def testEvalSimple(self):
        self.client = self.get_client()
        r = self.client.eval('return {1,2,3,4,5,6}',None)
        self.assertTrue(isinstance(r,list))
        self.assertEqual(len(r),6)
        
    def testTable(self):
        self.client = self.get_client()
        r = self.client.eval('return {name="mars", mass=0.11, orbit = 1.52}',
                             None)
        self.assertTrue(isinstance(r,list))
        self.assertEqual(len(r), 0)
    
    def testType(self):
        r = self.client.eval("""return redis.call('type',KEYS[1])['ok']""",
                             'sjdcvsd')
        self.assertEqual(r,b'none')
        
    def testEvalSha(self):
        self.assertEqual(self.client.script_flush(), True)
        re = Receiver()
        redis.redis_after_receive.connect(re)
        r = self.client.script_call('test_script', None,
                                    json.dumps([1,2,3,4,5,6]))
        self.assertEqual(r,[1,2,3,4,5,6])
        self.assertEqual(len(re.get()),2)
        r = self.client.script_call('test_script', None, json.dumps([1,2,3]))
        self.assertEqual(r,[1,2,3])
        self.assertEqual(len(re.get()),1)
        self.assertEqual(self.client.script_flush(), True)
        self.assertEqual(len(re.get()), 1)
        r = self.client.script_call('test_script', None,
                                    json.dumps([1,2,3,4,5,'bla']))
        self.assertEqual(r,[1,2,3,4,5, 'bla'])
        self.assertEqual(len(re.get()), 2)
        redis.redis_after_receive.disconnect(re)
        
    def testEvalShaPipeline(self):
        self.assertEqual(self.client.script_flush(),True)
        pipe = self.client.pipeline()
        pipe.script_call('test_script',None,['ciao']).sadd('bla','foo')
        result = pipe.execute()
        self.assertTrue(isinstance(result[0],NoScriptError))
        self.assertTrue(result[1])

    def testEvalShaPipeline(self):
        # Flush server scripts
        self.assertEqual(self.client.script_flush(), True)
        pipe = self.client.pipeline()
        pipe.script_call('test_script', None, json.dumps({'foo':[1,2]}))\
            .sadd('bla','foo')
        result = pipe.execute(load_script=True)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0],{'foo':[1,2]})
    

        
class TestStruct(client.TestCase):
    
    def testDouble(self):
        c = self.client
        numbers = [3, -4.45, 5.89, 23434234234.394, -239453.49]
        for n in numbers:
            # need this trick for python 2
            sn = str(n)
            n = float(sn)
            # pack in lua
            r = c.eval("return struct.pack('>d',ARGV[1])", None, sn)
            self.assertEqual(len(r), 8)
            # pack in python
            pr = struct.pack('>d', n)
            self.assertEqual(r, pr)
            # unpack lua-lua
            rn = float(c.eval("return '' .. struct.unpack('>d',ARGV[1])",
                              None, r))
            # unpack python-lua
            prn = float(c.eval("return '' .. struct.unpack('>d',ARGV[1])",
                              None, pr))
            self.assertAlmostEqual(n,rn,4)
            self.assertAlmostEqual(prn,rn,4)
            # unpack python python
            pprn = struct.unpack('>d',pr)[0]
            self.assertAlmostEqual(prn,pprn,4)
            
    def testInt(self):
        c = self.client
        numbers = [3,4,5,6,7,-10,-23456]
        for n in numbers:
            # pack in lua
            r = c.eval("return struct.pack('>i',ARGV[1])", None, n)
            self.assertEqual(len(r),4)
            # pack in python
            pr = struct.pack('>i',n)
            self.assertEqual(r,pr)
            # unpack lua-lua
            rn = int(c.eval("return '' .. struct.unpack('>i',ARGV[1])",
                              None, r))
            # unpack python-lua
            prn = int(c.eval("return '' .. struct.unpack('>i',ARGV[1])",
                              None, pr))
            self.assertEqual(n,rn,4)
            self.assertEqual(prn,rn,4)
            # unpack python python
            pprn = struct.unpack('>i',pr)[0]
            self.assertEqual(prn,pprn,4)
    
    def __testNaN(self):
        c = self.client
        n = float('nan')
        pn = c.eval("return struct.pack('>d',ARGV[1])", n)
        ppn = struct.pack('>d',n)
        #pnr = float(c.eval("return '' .. struct.unpack('>d',NaN)"))
        pnr = c.eval("return '' .. struct.unpack('>d',ARGV[1])", ppn)
        self.asserEqual(pnr,'nan')
        