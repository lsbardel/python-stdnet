from stdnet.lib import redis
import unittest as test

DBTEST = 13


ResponseError = redis.ResponseError

class BaseTest(test.TestCase):
    
    def get_client(self, pool = None):
        return redis.Redis(host='localhost', port=6379, db=DBTEST,
                           connection_pool=pool)
