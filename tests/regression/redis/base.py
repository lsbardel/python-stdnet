__test__ = False
from stdnet.lib import redis
from stdnet import test, getdb
from stdnet.utils import flatzset

def get_version(info):
    if 'redis_version' in info:
        return info['redis_version']
    else:
        return info['Server']['redis_version']


class TestCase(test.TestCase):
    
    def setUp(self):
        self.client = self.get_client()
        return self.client.flushdb()
    
    def tearDown(self):
        return self.client.flushdb()
        
    def get_client(self, pool = None, build = False):
        if pool or build:
            return gedb(self.backend.connection_string).client
        else:
            return self.backend.client
        
    def make_hash(self, key, d):
        for k,v in d.items():
            self.client.hset(key, k, v)

    def make_list(self, name, l):
        l = tuple(l)
        self.client.rpush(name, *l)
        self.assertEqual(self.client.llen(name),len(l))
            
    def make_set(self, name, l):
        for i in l:
            self.client.sadd(name, i)

    def make_zset(self, name, d):
        self.client.zadd(name, *flatzset(kwargs=d))
        