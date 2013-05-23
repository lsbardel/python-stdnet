'''Test the asynchronous redis client'''
import pulsar

from stdnet.utils import test
from stdnet.utils.async import async_binding

from examples.data import FinanceTest 

def check_connection(self, command_name):
    redis = self.mapper.default_backend.client
    client = redis.connection_pool
    self.assertIsInstance(client, pulsar.Client)
    for pool in client.connection_pools.values():
        for conn in pool._concurrent_connections:
            consumer = conn.current_consumer
            request = consumer.current_request
            self.assertEqual(client.available_connections, 1)
    
    
@test.skipUnless(async_binding, 'Requires asynchronous binding')
class TestRedisAsyncClient(test.TestWrite):
    multipledb = 'redis'
    
    @classmethod
    def after_setup(cls):
        return cls.data.create(cls)
    
    @classmethod
    def backend_params(cls):
        return {'timeout': 0}
        
    def test_client(self):
        redis = self.mapper.default_backend.client
        ping = redis.execute_command('PING', full_response=True)
        result = yield ping.on_finished
        self.assertTrue(result)
        
        