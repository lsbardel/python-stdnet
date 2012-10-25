import socket

from stdnet.test import mock

from .base import TestCase, redis


class DummyConnection(redis.Connection):
    request_class = redis.RedisRequest


def write_mock():
    parent = mock.MagicMock()
    parent.sendall = mock.MagicMock(side_effect=socket.error)
    parent.close = mock.MagicMock(side_effect=socket.error)
    return parent

def read_mock():
    parent = mock.MagicMock()
    parent.recv = mock.MagicMock(side_effect=socket.error)
    parent.close = mock.MagicMock(side_effect=socket.error)
    return parent


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
        
    def test_redisRequest(self):
        rpy = redis.Redis(self.get_pool())
        request = rpy.request('PING')
        self.assertTrue(isinstance(request, redis.RedisRequest))
        self.assertFalse(request.is_pipeline)
        self.assertEqual(request.num_responses, 1)
        self.assertRaises(NotImplementedError, request.execute)
        self.assertRaises(NotImplementedError, request.pool)
        self.assertEqual(request.raw_response, b'')
        self.assertEqual(str(request), 'PING()')
    
    def testFailWrite(self):
        request = self.client.request('PING')
        request.connection._sock = write_mock()
        self.assertTrue(request.execute())
        self.assertEqual(request.tried, 2)
        
    def testFailWriteAll(self):
        request = self.client.request('PING')
        request.retry = 1
        request.connection._sock = write_mock()
        self.assertRaises(socket.error, request.execute)
        self.assertEqual(request.tried, 1)
        self.assertEqual(request.connection, None)
        
    def testFailClose(self):
        client = self.client.clone()
        client.parse_response = mock.MagicMock(return_value=RuntimeError())
        request = client.request('PING')
        self.assertRaises(RuntimeError, request.execute)
        self.assertEqual(request.tried, 1)
        self.assertEqual(request.connection, None)