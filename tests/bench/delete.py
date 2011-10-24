'''Benchmark deletion of instances.'''
from stdnet import test, transaction
from stdnet.utils import populate, zip

from examples.data import FinanceTest, Instrument, Fund, Position


class DeleteTest(FinanceTest):
    
    def setUp(self):
        self.data.create()
                        
    def testDelete(self):
        for inst in Instrument.objects.all():
            inst.delete()
            
    def testDeleteTransaction(self):
        with transaction(Instrument, Fund, Position) as t:
            for inst in Instrument.objects.all():
                inst.delete(t)
