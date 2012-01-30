'''Benchmark creation of instances.'''
from datetime import date

from stdnet import test
from stdnet.apps.columnts import ColumnTS

from examples.data import tsdata

class TestCase(test.TestCase):
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        
    def setUp(self):
        self.backend.load_scripts()
        
    def startUp(self):
        session = self.session()
        self.ts = session.add(ColumnTS())
        self.ts.update(self.data.values)
        

class CreateTest(TestCase):
    
    def testCommit(self):
        self.ts.session.commit()
        
        
class RedisCommandBecnhmark(TestCase):
    
    def setUp(self):
        self.backend.load_scripts()
        c = self.backend.client
        self.slowlog = c.config_get('slowlog-log-slower-than')
        c.config_set('slowlog-log-slower-than', 0)
        self.backend.client.slowlog_reset()
                
    def testCommitRedis(self):
        self.ts.session.commit()
        
    def getTime(self, dt):
        log = self.backend.client.slowlog_get(30)
        self.assertEqual(len(log),4)
        self.assertEqual(log[0]['command'],'EXEC')
        self.assertEqual(log[1]['command'],'EVAL')
        self.assertEqual(log[2]['command'],'MULTI')
        self.assertEqual(log[3]['command'],'SLOWLOG')
        self.backend.client.slowlog_reset()
        ms = log[1]['microseconds']
        return 0.000001*ms
        
    def tearDown(self):
        self.assertTrue(
                self.backend.client.config_set('slowlog-log-slower-than',
                               self.slowlog['slowlog-log-slower-than']))
        
    
    