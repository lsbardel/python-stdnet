import unittest
from itertools import izip
from timeit import default_timer as timer

from stdnet.main import getdb
from stdnet.utils import populate, date2timestamp, OrderedDict
from stdnet import settings_test


SIZEMAP = 100

test_keys   = populate('date',SIZEMAP,converter=date2timestamp)
test_values = populate('float',SIZEMAP)


def available(cache):
    if not cache:
        return False
    cache.set("stdnet-test",1)
    avail = cache.get("stdnet-test") or False
    cache.delete("stdnet-test")
    return avail

class testLocMem(unittest.TestCase):
    keyset1   = 'stdnet-test-set'
    keyset2   = 'stdnet-test-set2'
    keymap    = 'stdnet-maptest2'
    map_types = [1]
    
    def setUp(self):
        pass
    
    def _testSet(self):
        cache = self.cache
        key   = self.keyset1
        self.assertTrue(cache.sadd(key,'first-entry'))
        self.assertTrue(cache.sadd(key,'another one'))
        self.assertTrue(cache.sadd(key,'the third one'))
        data = cache.smembers(key)
        self.assertEqual(len(data),3)
        self.assertFalse(cache.sadd(key,'first-entry'))
        
    def _testDelete(self):
        cache = self.cache
        key   = self.keyset1
        self.assertTrue(cache.sadd(key,'first-entry'))
        self.assertTrue(cache.sadd(key,'second-entry'))
        a = cache.delete(key)
        b = 1
        
    def _testBadSet(self):
        cache = self.cache
        key   = self.keyset2
        self.assertTrue(cache.add(key,'bla'))
        cache.sadd(key,'bla2')
         
    def testMap(self):
        cache  = self.cache
        id     = self.keymap
        
        d      = OrderedDict()
        added  = 0
        map    = cache.map(id, typ)
        print("\nTesting map")
        
        t1 = timer()
        for ts,v in izip(test_keys,test_values):
            added += map.add(ts,v)
        dt = timer() - t1
        print("Added %s items in %s seconds" % (SIZEMAP,dt))
        
        for ts,v in izip(test_keys,test_values):
            d[ts] = v
            
        self.assertTrue(len(map)>0)
        self.assertEqual(len(map),added)
        self.assertEqual(len(map),len(d))
        
        t1 = timer()
        mkeys = map.keys()
        dt = timer() - t1
        print("Got %s keys in %s seconds" % (len(mkeys),dt))
        
        t1 = timer()
        mkeys = list(map.values())
        dt = timer() - t1
        print("Got %s values in %s seconds" % (len(mkeys),dt))
        
        kp = None
        for k,v in map.items():
            k = int(k)
            v = float(v)
            if kp is not None:
                self.assertTrue(k>kp)
            vd = d[k]
            self.assertAlmostEqual(v,vd)
            kp = k
        cache.delete(id)
        
    def tearDown(self):
        cache = self.cache
        cache.delete(self.keymap)
        #cache.delete(self.keyset1,self.keyset2,self.keymap)


cache_memcached = get_cache(settings_test.memcached)

if available(cache_memcached):    
    class testMemcached(testLocMem):
        cachename = 'Memcached'
        cache = cache_memcached
        

cache_redis = get_cache(settings_test.redis)

if available(cache_redis):
    
    class testRedis(testLocMem):
        cachename = 'Redis'
        cache = cache_redis
        map_types = [1,2]
    
    
