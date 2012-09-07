import os

from stdnet import test


@test.skipUnless(os.environ['stdnet_test_suite'] == 'pulsar', 'Requires Pulsar')
class TestAsyncRedis(test.TestCase):
    
    def backend_params(self):
        from stdnet.lib.redis.async import RedisConnection
        return {'connection_class': RedisConnection}
    
    def setUp(self):
        self.client = self.backend.client
        
    def clear_all(self):
        return self.backend.flush(pattern=self.prefix + '*')
    
    def _post_teardown(self):
        yield self.clear_all()
            
    def testMeta(self):
        from stdnet.lib.redis.async import RedisConnection
        client = self.backend.client
        self.assertEqual(client.connection_pool.connection_class,
                         RedisConnection)
        
    def testSimple(self):
        client = self.backend.client
        request = client.ping()
        self.assertEqual(request.command_name, 'PING')
        self.assertEqual(str(request), 'PING')
        yield request
        self.assertEqual(request.result, True)
        request = client.echo('ciao')
        self.assertEqual(str(request), "ECHO('ciao',)")
        yield request
        self.assertEqual(request.result, b'ciao')
    