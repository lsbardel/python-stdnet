from .base import TestCase, redis, ConnectionError


class DummyConnection(object):
    def __init__(self, pool, **kwargs):
        self.pool = pool
        self.kwargs = kwargs


class ConnectionPoolTestCase(TestCase):
    
    def get_pool(self, connection_info=None, max_connections=None):
        connection_info = connection_info or {'a': 1, 'b': 2, 'c': 3}
        pool = redis.ConnectionPool('localhost:0',
            connection_class=DummyConnection, max_connections=max_connections,
            **connection_info)
        return pool

    def test_connection_creation(self):
        connection_info = {'foo': 'bar', 'biz': 'baz', 'encoding': 'utf-8'}
        pool = self.get_pool(connection_info=connection_info)
        connection = pool.get_connection()
        self.assertEquals(connection.kwargs, connection_info)

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        self.assert_(c1 != c2)

    def test_max_connections(self):
        pool = self.get_pool(max_connections=2)
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        self.assertRaises(ConnectionError, pool.get_connection)

    def test_release(self):
        pool = self.get_pool()
        c1 = pool.get_connection()
        pool.release(c1)
        c2 = pool.get_connection()
        self.assertEquals(c1, c2)