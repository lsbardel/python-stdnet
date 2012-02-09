__benchmark__ = True
from datetime import datetime, date

from stdnet import test
from stdnet import orm

from examples.data import hash_data


class TestCase(test.TestCase):
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data = hash_data(size = size, fieldtype = 'date')
        
    def setUp(self):
        self.backend.load_scripts()
        

######### Create TEST CASES

class CreateTest(TestCase):
    '''Create the timeseries'''
    def startUp(self):
        session = self.session()
        # start the transaction
        self.transaction = session.begin()
        self.ts = ts = session.add(orm.TS())
        self.ts.update(self.data.items())
        self.assertTrue(self.ts.cache.toadd)
        
    def testCommit(self):
        self.ts.session.commit()
        
    def getInfo(self, delta, dt, info):
        pass
    
    def getSummary(self, number, total_time, total_time2, info):
        return info