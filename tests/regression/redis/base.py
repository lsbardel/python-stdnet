from stdnet.lib import redis
from stdnet.lib.exceptions import *
import unittest as test

DBTEST = 13


class BaseTest(test.TestCase):
    
    def get_client(self, pool = None):
        return redis.Redis(host='localhost', port=6379, db=DBTEST,
                           connection_pool=pool)
