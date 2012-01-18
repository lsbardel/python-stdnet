'''Benchmark creation of instances.'''
from stdnet import test

from examples.data import FinanceTest, Instrument, Fund, Position


class CreateTest(FinanceTest):
              
    def testCreate(self):
        self.data.create(self, False)
        
    def testCreateTransaction(self):
        self.data.create(self, True)
        