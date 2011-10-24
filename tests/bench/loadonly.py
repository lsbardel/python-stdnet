'''Benchmark deletion of instances.'''
from stdnet import test, transaction
from stdnet.utils import populate, zip

from examples.data import FinanceTest, Instrument, Fund, Position


class LoadOnlyTest(FinanceTest):
    
    def setUp(self):
        self.data.create()
                        
    def testLoadOnlyId(self):
        inst = list(Instrument.objects.all().load_only('id'))
            
    def testLoadOnlyName(self):
        inst = list(Instrument.objects.all().load_only('name'))
    
    def testLoadAll(self):
        inst = list(Instrument.objects.all())