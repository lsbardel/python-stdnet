from stdnet.lib import redis
from stdnet import test, getdb
from stdnet.conf import settings


ResponseError = redis.ResponseError
RedisError = redis.RedisError


def makeredis(pool = None):
    cursor = getdb(format(settings.DEFAULT_BACKEND), decode = 1)
    return cursor.redispy


def get_version(info):
    if 'redis_version' in info:
        return info['redis_version']
    else:
        return info['Server']['redis_version']


class BaseTest(test.TestCase):
    _client_no_pool = makeredis()
    
    def get_client(self, pool = None, build = False):
        if pool or build:
            return makeredis(pool)
        else:
            return self._client_no_pool
