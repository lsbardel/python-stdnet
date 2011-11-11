from stdnet.lib import redis
from stdnet import test, getdb
from stdnet.conf import settings
from stdnet.lib.async import AsyncRedisConnection


def makeredis(pool = None):
    cursor = getdb(format(settings.DEFAULT_BACKEND), decode = 1,
                   connection_class = AsyncRedisConnection)
    return cursor.client


class AsyncAssert(object):
    __slots__ = ('test','name')
    
    def __init__(self, test, name = None):
        self.test = test
        self.name = name
        
    def __getattr__(self, name):
        return AsyncAssert(self.test,name)
    
    def __call__(self, *args, **kwargs):
        func = getattr(self.test,self.name)
        return make_async(func(*args,**kwargs)).add_callback(self._check_result)
    
    def _check_result(self, result):
        pass
    

class RedisCommands(test.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.client = makeredis()
        
    def setUp(self):
        self.async = AsyncAssert(self)

    def test_get_and_set(self):
        yield self.async.assertEquals(self.client.get('a'), None)