from hashlib import sha1
import json
import struct

from stdnet.conf import settings
from stdnet.lib import redis

from .base import TestCase


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


class ScriptingCommandsTestCase(TestCase):
    tag = 'script'
    default_run = False

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
    
    def testType(self):
        r = self.client.eval("""return redis.call('type',KEYS[1])['ok']""",
                             'sjdcvsd')
        self.assertEqual(r,b'none')
        
    def testScript(self):
        script = redis.get_script('test_script')
        self.assertTrue(script.script)
        sha = sha1(script.script.encode('utf-8')).hexdigest()
        self.assertEqual(script.sha1,sha)
        
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
    
    def testMove2Set(self):
        self.client.sadd('foo', 1, 2, 3, 4, 5)
        self.client.lpush('bla', 4, 5, 6, 7, 8)
        r = self.client.script_call('move2set', ('foo', 'bla'), 's')
        self.assertEqual(len(r),2)
        self.assertEqual(r[0],2)
        self.assertEqual(r[1],1)
        self.client.sinterstore('res1','foo','bla')
        self.client.sunionstore('res2','foo','bla')
        m1 = sorted((int(r) for r in self.client.smembers('res1')))
        m2 = sorted((int(r) for r in self.client.smembers('res2')))
        self.assertEqual(m1,[4,5])
        self.assertEqual(m2,[1,2,3,4,5,6,7,8])
    
    def testMove2ZSet(self):
        self.client.zadd('foo',1,'a',2,'b',3,'c',4,'d',5,'e')
        self.client.lpush('bla','d','e','f','g')
        r = self.client.script_call('move2set',('foo','bla'),'z')
        self.assertEqual(len(r),2)
        self.assertEqual(r[0],2)
        self.assertEqual(r[1],1)
        self.client.zinterstore('res1','foo','bla')
        self.client.zunionstore('res2','foo','bla')
        m1 = sorted(self.client.zrange('res1',0,-1))
        m2 = sorted(self.client.zrange('res2',0,-1))
        self.assertEqual(m1,[b'd',b'e'])
        self.assertEqual(m2,[b'a',b'b',b'c',b'd',b'e',b'f',b'g'])
        
    def testMoveSetSet(self):
        self.client.sadd('foo',1,2,3,4,5)
        self.client.sadd('bla',4,5,6,7,8)
        r = self.client.script_call('move2set',('foo','bla'),'s')
        self.assertEqual(len(r),2)
        self.assertEqual(r[0],2)
        self.assertEqual(r[1],0)
        
    def testMove2List2(self):
        self.client.lpush('foo',1,2,3,4,5)
        self.client.lpush('bla',4,5,6,7,8)
        r = self.client.script_call('move2set',('foo','bla'),'s')
        self.assertEqual(len(r),2)
        self.assertEqual(r[0],2)
        self.assertEqual(r[1],2)
        
    def testKeyInfo(self):
        self.client.set('planet', 'mars')
        self.client.lpush('foo', 1, 2, 3, 4, 5)
        self.client.lpush('bla', 4, 5, 6, 7, 8)
        keys = list(self.client.script_call('keyinfo', (), '*'))
        self.assertEqual(len(keys), 3)
        d = dict(((k.id, k) for k in keys))
        self.assertEqual(d['planet'].length,4)
        self.assertEqual(d['planet'].type,'string')
        self.assertEqual(d['planet'].encoding,'raw')
        
    def testKeyInfo2(self):
        self.client.set('planet','mars')
        self.client.lpush('foo',1,2,3,4,5)
        self.client.lpush('bla',4,5,6,7,8)
        keys = list(self.client.script_call('keyinfo',('planet','bla')))
        self.assertEqual(len(keys),2)
        
    # ZSET SCRIPTING COMMANDS
    
    def testzpopbyrank(self):
        self.client.zadd('foo',1,'a',2,'b',3,'c',4,'d',5,'e')
        res = self.client.zpopbyrank('foo',0)
        rem = self.client.zrange('foo',0,-1)
        self.assertEqual(len(rem),4)
        self.assertEqual(rem,[b'b',b'c',b'd',b'e'])
        self.assertEqual(res,[b'a'])
        res = self.client.zpopbyrank('foo',0,2)
        self.assertEqual(res,[b'b',b'c',b'd'])
        rem = self.client.zrange('foo',0,-1)
        self.assertEqual(rem,[b'e'])
        
    def testzpopbyscore(self):
        self.client.zadd('foo',1,'a',2,'b',3,'c',4,'d',5,'e')
        res = self.client.zpopbyscore('foo', 2)
        rem = self.client.zrange('foo',0,-1)
        self.assertEqual(len(rem),4)
        self.assertEqual(rem,[b'a',b'c',b'd',b'e'])
        self.assertEqual(res,[b'b'])
        res = self.client.zpopbyscore('foo',0,4.5)
        self.assertEqual(res,[b'a',b'c',b'd'])
        rem = self.client.zrange('foo',0,-1)
        self.assertEqual(rem,[b'e'])
        

class TestForCoverage(TestCase):
    
    def test_bad_script_call(self):
        self.assertRaises(redis.RedisError, self.client.script_call, 'foo', ())
        
        
class TestStruct(TestCase):
    
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
        