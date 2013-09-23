'''Test additional commands for redis client.'''
import json
from hashlib import sha1

from stdnet import getdb
from stdnet.backends import redisb
from stdnet.utils import test, flatzset

def get_version(info):
    if 'redis_version' in info:
        return info['redis_version']
    else:
        return info['Server']['redis_version']
    
    
class test_script(redisb.RedisScript):
    script = (redisb.read_lua_file('commands.utils'),
              '''\
local js = cjson.decode(ARGV[1])
return cjson.encode(js)''')
    
    def callback(self, request, result, args, **options):
        return json.loads(result.decode(request.encoding))


class TestCase(test.TestWrite):
    multipledb = 'redis'
    
    def setUp(self):
        client = self.backend.client
        self.client = client.prefixed(self.namespace)
    
    def tearDown(self):
        return self.client.flushdb()
        
    def make_hash(self, key, d):
        for k, v in d.items():
            self.client.hset(key, k, v)

    def make_list(self, name, l):
        l = tuple(l)
        self.client.rpush(name, *l)
        self.assertEqual(self.client.llen(name), len(l))

    def make_zset(self, name, d):
        self.client.zadd(name, *flatzset(kwargs=d))
        
        
class TestExtraClientCommands(TestCase):
    
    def test_coverage(self):
        c = self.backend.client
        self.assertEqual(c.prefix, '')
        size = yield c.dbsize()
        self.assertTrue(size >= 0)
        
    def test_script_meta(self):
        script = redisb.get_script('test_script')
        self.assertTrue(script.script)
        sha = sha1(script.script.encode('utf-8')).hexdigest()
        self.assertEqual(script.sha1,sha)
        
    def test_del_pattern(self):
        c = self.client
        items = ('bla',1,
                 'bla1','ciao',
                 'bla2','foo',
                 'xxxx','moon',
                 'blaaaaaaaaaaaaaa','sun',
                 'xyyyy','earth')
        yield self.async.assertTrue(c.execute_command('MSET', *items))
        N = yield c.delpattern('bla*')
        self.assertEqual(N, 4)
        yield self.async.assertFalse(c.exists('bla'))
        yield self.async.assertFalse(c.exists('bla1'))
        yield self.async.assertFalse(c.exists('bla2'))
        yield self.async.assertFalse(c.exists('blaaaaaaaaaaaaaa'))
        yield self.async.assertEqual(c.get('xxxx'), b'moon')
        N = yield c.delpattern('x*')
        self.assertEqual(N, 2)
        
    def testMove2Set(self):
        yield self.multi_async((self.client.sadd('foo', 1, 2, 3, 4, 5),
                                self.client.lpush('bla', 4, 5, 6, 7, 8)))
        r = yield self.client.execute_script('move2set', ('foo', 'bla'), 's')
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 2)
        self.assertEqual(r[1], 1)
        yield self.multi_async((self.client.sinterstore('res1', 'foo', 'bla'),
                                self.client.sunionstore('res2', 'foo', 'bla')))
        m1 = yield self.client.smembers('res1')
        m2 = yield self.client.smembers('res2')
        m1 = sorted((int(r) for r in m1))
        m2 = sorted((int(r) for r in m2))
        self.assertEqual(m1, [4,5])
        self.assertEqual(m2, [1,2,3,4,5,6,7,8])
    
    def testMove2ZSet(self):
        client = self.client
        yield self.multi_async((client.zadd('foo',1,'a',2,'b',3,'c',4,'d',5,'e'),
                                client.lpush('bla','d','e','f','g')))
        r = yield client.execute_script('move2set', ('foo','bla'), 'z')
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 2)
        self.assertEqual(r[1], 1)
        yield self.multi_async((client.zinterstore('res1', ('foo', 'bla')),
                                client.zunionstore('res2', ('foo', 'bla'))))
        m1 = yield client.zrange('res1', 0, -1)
        m2 = yield client.zrange('res2', 0, -1)
        self.assertEqual(sorted(m1), [b'd', b'e'])
        self.assertEqual(sorted(m2), [b'a',b'b',b'c',b'd',b'e',b'f',b'g'])
        
    def testMoveSetSet(self):
        r = yield self.multi_async((self.client.sadd('foo',1,2,3,4,5),
                                    self.client.sadd('bla',4,5,6,7,8)))
        r = yield self.client.execute_script('move2set', ('foo', 'bla'), 's')
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 2)
        self.assertEqual(r[1], 0)
        
    def testMove2List2(self):
        yield self.multi_async((self.client.lpush('foo',1,2,3,4,5),
                                self.client.lpush('bla',4,5,6,7,8)))
        r = yield self.client.execute_script('move2set', ('foo','bla'), 's')
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 2)
        self.assertEqual(r[1], 2)
        
    def test_bad_execute_script(self):
        self.assertRaises(redisb.RedisError, self.client.execute_script, 'foo', ())
        
    # ZSET SCRIPTING COMMANDS
    def test_zdiffstore(self):
        yield self.multi_async((self.make_zset('aa', {'a1': 1, 'a2': 1, 'a3': 1}),
                                self.make_zset('ba', {'a1': 2, 'a3': 2, 'a4': 2}),
                                self.make_zset('ca', {'a1': 6, 'a3': 5, 'a4': 4})))
        n = yield self.client.zdiffstore('za', ['aa', 'ba', 'ca'])
        self.assertEqual(n, 1)
        r = yield self.client.zrange('za', 0, -1, withscores=True)
        self.assertEquals(list(r), [(b'a2', 1)])
        
    def test_zdiffstore_withscores(self):
        yield self.multi_async((self.make_zset('ab', {'a1': 6, 'a2': 1, 'a3': 2}),
                                self.make_zset('bb', {'a1': 1, 'a3': 1, 'a4': 2}),
                                self.make_zset('cb', {'a1': 3, 'a3': 1, 'a4': 4})))
        n = yield self.client.zdiffstore('zb', ['ab', 'bb', 'cb'], withscores=True)
        self.assertEqual(n, 2)
        r = yield self.client.zrange('zb', 0, -1, withscores=True)
        self.assertEquals(list(r), [(b'a2', 1), (b'a1', 2)])
        
    def test_zdiffstore2(self):
        c = self.client
        yield self.multi_async((c.zadd('s1', 1, 'a', 2, 'b', 3, 'c', 4, 'd'),
                                c.zadd('s2', 6, 'a', 9, 'b', 100, 'c')))
        r = yield c.zdiffstore('s3', ('s1', 's2'))
        self.async.assertEqual(c.zcard('s3'), 1)
        r = yield c.zrange('s3', 0, -1)
        self.assertEqual(r, [b'd'])
        
    def test_zdiffstore_withscores2(self):
        c = self.client
        yield self.multi_async((c.zadd('s1', 1, 'a', 2, 'b', 3, 'c', 4, 'd'),
                                c.zadd('s2', 6, 'a', 2, 'b', 100, 'c')))
        r = yield c.zdiffstore('s3', ('s1', 's2'), withscores=True)
        self.async.assertEqual(c.zcard('s3'), 3)
        r = yield c.zrange('s3', 0, -1, withscores=True)
        self.assertEqual(dict(r), {b'a': -5.0, b'c': -97.0, b'd': 4.0})
        
    def test_zpop_byrank(self):
        yield self.client.zadd('foo',1,'a',2,'b',3,'c',4,'d',5,'e')
        res = yield self.client.zpopbyrank('foo',0)
        rem = yield self.client.zrange('foo',0,-1)
        self.assertEqual(len(rem),4)
        self.assertEqual(rem,[b'b',b'c',b'd',b'e'])
        self.assertEqual(res,[b'a'])
        res = yield self.client.zpopbyrank('foo',0,2)
        self.assertEqual(res,[b'b',b'c',b'd'])
        rem = yield self.client.zrange('foo',0,-1)
        self.assertEqual(rem,[b'e'])
        
    def test_zpop_byscore(self):
        yield self.client.zadd('foo', 1, 'a', 2, 'b', 3, 'c', 4, 'd', 5, 'e')
        res = yield self.client.zpopbyscore('foo', 2)
        rem = yield self.client.zrange('foo', 0, -1)
        self.assertEqual(len(rem), 4)
        self.assertEqual(rem, [b'a', b'c', b'd', b'e'])
        self.assertEqual(res, [b'b'])
        res = yield self.client.zpopbyscore('foo', 0, 4.5)
        self.assertEqual(res, [b'a', b'c', b'd'])
        rem = yield self.client.zrange('foo', 0, -1)
        self.assertEqual(rem, [b'e'])