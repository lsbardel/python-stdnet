from stdnet.utils import PPath
from stdnet.conf import settings

p = PPath(__file__)
p.add(module = 'pulsar', up = 1, down = ('pulsar',))
import pulsar
from pulsar.apps.test import TestSuite

class TestServer(pulsar.SettingPlugin):
    name = "server"
    cli = ["-s", "--server"]
    desc = 'Backend server where to run tests.'
    default = settings.DEFAULT_BACKEND
    

if __name__ == '__main__':
    p = PPath(__file__)
    p.add(module = 'pulsar', up = 1, down = ('pulsar',))
    from pulsar.apps.test import TestSuite, TestOption
    
    suite = TestSuite(description = 'Stdnet Asynchronous test suite',
                      modules = (('tests.regression','tests'),
                                 ('stdnet.apps','tests')))
    
    suite.start()