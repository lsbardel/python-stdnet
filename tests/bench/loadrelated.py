'''Benchmark load realted.'''
from stdnet import test
from stdnet.utils import populate, zip

from examples.data import FinanceTest, Instrument, Fund, Position


class LoadRelatedTest(FinanceTest):
    
    def setUp(self):
        self.register()
        self.data.makePositions(self)
                        
    def testLoad(self):
        for p in Position.objects.query():
            self.assertTrue(p.instrument.name)
            
    def testLoadRelated(self):
        for p in Position.objects.query().load_related('instrument'):
            self.assertTrue(p.instrument.name)
    