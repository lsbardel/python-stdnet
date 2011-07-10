from stdnet.lib import redis
from stdnet import test, getdb
from stdnet.conf import settings


ResponseError = redis.ResponseError
RedisError = redis.RedisError

def makeredis(pool = None):
    cursor = getdb('{0}&decode=1'.format(settings.DEFAULT_BACKEND))
    return cursor.redispy


class BaseTest(test.TestCase):
    _client_no_pool = makeredis()
    
    def get_client(self, pool = None, build = False):
        if pool or build:
            return makeredis(pool)
        else:
            return self._client_no_pool
