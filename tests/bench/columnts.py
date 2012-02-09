'''Benchmark creation of instances.'''
from datetime import date

from stdnet import test
from stdnet.apps.columnts import ColumnTS

from examples.data import tsdata

class TestCase(test.TestCase):
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data1 = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        
    def setUp(self):
        self.backend.load_scripts()
    

class SlowLogMixin(object):
    command = 'EXEC'
    
    def resetlog(self):
        self.backend.load_scripts()
        c = self.backend.client
        self.slowlog = c.config_get('slowlog-log-slower-than')
        c.config_set('slowlog-log-slower-than', 0)
        self.backend.client.slowlog_reset()
    
    def getTime(self, dt):
        log = self.backend.client.slowlog_get(100)
        self.assertEqual(log[0]['command'],self.command)
        i = 0
        ms = log[0]['microseconds']
        if self.command == 'EXEC':
            i = 1
            while log[i]['command'] != 'MULTI':
                i += 1
            ms += log[i]['microseconds']
        else:
            self.assertEqual(len(log),2)
        self.assertEqual(log[i+1]['command'],'SLOWLOG')
        self.backend.client.slowlog_reset()
        return 0.000001*ms
    
    def tearDown(self):
        self.assertTrue(
                self.backend.client.config_set('slowlog-log-slower-than',
                               self.slowlog['slowlog-log-slower-than']))
        

######### Create TEST CASES

class CreateTest(TestCase):
    '''Create the timeseries'''
    def startUp(self):
        session = self.session()
        self.ts = session.add(ColumnTS())
        self.ts.update(self.data1.values)
        
    def testCommit(self):
        self.ts.session.commit()
        
    
class CreateTestRedis(SlowLogMixin,CreateTest):
    
    def startUp(self):
        super(CreateTestRedis,self).startUp()
        self.resetlog()
    
    
####### Stats Tests

class StatTest(TestCase):
    
    def setUp(self):
        self.backend.load_scripts()
        session = self.session()
        self.ts = session.add(ColumnTS())
        self.ts.update(self.data1.values)
        self.ts.session.commit()
        
    def testStats(self):
        self.ts.stats(0,-1)
        
        
class StatTestRedis(SlowLogMixin, StatTest):
    command = 'EVAL'
    
    def startUp(self):
        self.resetlog()
        
   
##### Merge Tests

class MergeTest(TestCase):
    
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data1 = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        cls.data2 = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        cls.data3 = tsdata(size = size, fields = ('a','b','c','d','f','g'))
        
    def setUp(self):
        self.backend.load_scripts()
        session = self.session()
        with session.begin():
            self.ts1 = session.add(ColumnTS())
            self.ts1.update(self.data1.values)
            self.ts2 = session.add(ColumnTS())
            self.ts2.update(self.data2.values)
            self.ts3 = session.add(ColumnTS())
            self.ts3.update(self.data3.values)
        self.assertTrue(self.ts1.size())
        self.assertTrue(self.ts2.size())
        self.assertTrue(self.ts3.size())
        
    def testMerge(self):
        ts = ColumnTS()
        ts.merge((self.ts1,1.5),(self.ts2,2),(self.ts3,-0.5))
        ts.session.commit()
        
        
class MergeTestRedis(SlowLogMixin, MergeTest):
    
    def startUp(self):
        self.resetlog()