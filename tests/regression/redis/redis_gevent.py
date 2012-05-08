try:
    import gevent
    from gevent import getcurrent
except:
    gevent = None
    
from stdnet import test

@test.unittest.skipUnless(gevent, 'Requires gevent')
class TestGeventRequest(test.TestCase):
    
    def backend_params(self):
        from stdnet.lib.redis.async import redis_gevent 
        return {'connection_class': redis_gevent.Connection}
    
    def testMeta(self):
        c = list(self.backend.client.connection_pool._in_use_connections)
        if c:
            g = getcurrent()
            request = c[0]._request
            