from .base import TestCase, redis


class DummyConnection(redis.Connection):
    pass


class ConnectionPoolTestCase(TestCase):
    
    def get_pool(self, connection_class=DummyConnection, **kwargs):
        return redis.ConnectionPool.create(address='localhost:0',
                                           connection_class=connection_class,
                                           **kwargs)
        
    def tearDown(self):
        for pool in redis.ConnectionPool.connection_pools.values():
            pool.disconnect()
        redis.ConnectionPool.connection_pools.clear()

    def test_connection_creation(self):
        pool = self.get_pool(socket_timeout=3)
        connection = pool.get_connection()
        self.assertEquals(connection.socket_timeout, 3)
        self.assertEquals(connection.encoding, 'utf-8')
        self.assertRaises(ValueError, redis.ConnectionPool.create)
        #pool2 = self.get_pool(socket_timeout=2)
        #self.assertNotEqual(pool, pool2)

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        self.assert_(c1 != c2)

    def test_max_connections(self):
        pool = self.get_pool(max_connections=2)
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        self.assertRaises(redis.RedisConnectionError, pool.get_connection)

    def test_release(self):
        pool = self.get_pool()
        c1 = pool.get_connection()
        pool.release(c1)
        c2 = pool.get_connection()
        self.assertEquals(c1, c2)