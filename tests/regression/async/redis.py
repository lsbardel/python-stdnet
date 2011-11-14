from stdnet.lib import redis
from stdnet import test, getdb
from stdnet.conf import settings
from stdnet.lib.async import AsyncRedisConnection


def makeredis(pool = None):
    cursor = getdb(format(settings.DEFAULT_BACKEND), decode = 1,
                   connection_class = AsyncRedisConnection)
    return cursor.client
    

class RedisCommands(test.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.client = makeredis()

    def test_get_and_set(self):
        yield self.async.assertTrue(self.client.set('a','ciao'))
        yield self.async.assertEquals(self.client.get('a'), 'ciao')