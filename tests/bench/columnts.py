'''Benchmark creation of instances.'''
from datetime import date

from stdnet import test
from stdnet.apps.columnts import ColumnTS

from examples.data import tsdata


class CreateTest(test.TestCase):
    
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data = tsdata(size = size)
        
    def setUp(self):
        self.backend.load_scripts()
        
    def startUp(self):
        session = self.session()
        self.ts = session.add(ColumnTS())
        self.ts.update(self.data.values)
        
    def testCommit(self):
        self.ts.session.commit()
        