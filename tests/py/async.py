'''Asynchronous pulsar connection'''
import os

from stdnet import BackendRequest
from stdnet.utils import test

from examples.models import SimpleModel


@test.skipUnless(os.environ['stdnet_test_suite'] == 'pulsar', 'Requires Pulsar')
class CleanAsync(test.CleanTestCase):
    multipledb = 'redis'
    
    @classmethod
    def backend_params(cls):
        from stdnet.lib.redis.async import RedisConnection
        return {'connection_class': RedisConnection}
    

class a:
#class TestAsyncRedis(CleanAsync):
    
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
    

class b:    
#class TestAsyncOdm(CleanAsync):
    model = SimpleModel
    
    def addData(self):
        session=  self.session()
        with session.begin() as t:
            t.add(SimpleModel(code='pluto', group='planet'))
            t.add(SimpleModel(code='venus', group='planet'))
        self.assertFalse(t.commands)
        self.assertTrue(t.pending)
        yield t.pending
        self.assertTrue(t.commands)
        self.assertFalse(t.pending)
        
    def testEmptyQuery(self):
        session = self.session()
        qs = session.query(SimpleModel).all()
        self.assertTrue(isinstance(qs, BackendRequest))
        yield qs
        self.assertEqual(qs.result, [])
        
    def testaddData(self):
        yield self.addData()
        session = self.session()
        outcome = session.query(SimpleModel).count()
        yield outcome
        self.assertEqual(outcome.result, 2)
        
    def testqueryData(self):
        yield self.addData()
        session = self.session()
        outcome = session.query(SimpleModel).all()
        yield outcome
        self.assertEqual(len(outcome.result), 2)