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
        
    def load_data(self):
        pass
        
    def startUp(self):
        session = self.session()
        self.ts = session.add(ColumnTS())
        self.ts.update(self.data.values)
        self.load_data()
    

class SlowLogTestCase(TestCase):
    command = 'EXEC'
    
    def setUp(self):
        self.backend.load_scripts()
        c = self.backend.client
        self.slowlog = c.config_get('slowlog-log-slower-than')
        c.config_set('slowlog-log-slower-than', 0)
        self.backend.client.slowlog_reset()
    
    def getTime(self, dt):
        log = self.backend.client.slowlog_get(100)
        self.assertEqual(log[0]['command'],self.command)
        if self.command == 'EXEC':
            i = 1
            while log[i]['command'] != 'MULTI':
                i += 1
        else:
            self.assertEqual(len(log),2)
            i = 0
        self.assertEqual(log[i+1]['command'],'SLOWLOG')
        self.backend.client.slowlog_reset()
        ms = log[1]['microseconds']
        return 0.000001*ms
    
    def tearDown(self):
        self.assertTrue(
                self.backend.client.config_set('slowlog-log-slower-than',
                               self.slowlog['slowlog-log-slower-than']))
        

######### TEST CASES

class CreateTest(TestCase):
    
    def testCommit(self):
        self.ts.session.commit()
        
    
class CreateRedisTim(SlowLogTestCase):
    
    def testCommitRedis(self):
        self.ts.session.commit()
    
    
class OperationTest(TestCase):
    
    def load_data(self):
        self.ts.session.commit()
        
    def testStats(self):
        self.ts.stats(0,-1)
        
        
class OperationTestRedis(SlowLogTestCase):
    command = 'EVAL'
    def load_data(self):
        self.ts.session.commit()
        self.backend.client.slowlog_reset()
        
    def testStatsRedis(self):
        self.ts.stats(0,-1)
        
    