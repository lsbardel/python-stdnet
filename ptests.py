from stdnet.utils import PPath
from stdnet.conf import settings

p = PPath(__file__)
p.add(module = 'pulsar', up = 1, down = ('pulsar',))
import pulsar
from pulsar.apps.test import TestSuite, TestOptionPlugin
from pulsar.apps.test.plugins import bench


class TestServer(TestOptionPlugin):
    name = "server"
    flags = ["-s", "--server"]
    desc = 'Backend server where to run tests.'
    default = settings.DEFAULT_BACKEND
    
    def configure(self, cfg):
        settings.DEFAULT_BACKEND = cfg.server
        
    

if __name__ == '__main__':
    p = PPath(__file__)
    p.add(module = 'pulsar', up = 1, down = ('pulsar',))
    
    suite = TestSuite(description = 'Stdnet Asynchronous test suite',
                      modules = ('tests','stdnet.apps'),
                      plugins = (TestServer(),bench.BenchMark(),)
                      )
    
    suite.start()