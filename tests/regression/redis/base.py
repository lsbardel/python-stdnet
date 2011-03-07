from stdnet.lib import redis
from stdnet import test

DBTEST = 13


ResponseError = redis.ResponseError

def makeredis(pool = None):
    return redis.Redis(host='localhost', port=6379, db=DBTEST, connection_pool=pool)


class BaseTest(test.TestCase):
    _client_no_pool = makeredis()
    
    def get_client(self, pool = None, build = False):
        if pool or build:
            return makeredis(pool)
        else:
            return self._client_no_pool
