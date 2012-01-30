'''Benchmark deletion of instances.'''
from stdnet import test
from stdnet.utils import populate, zip

from examples.data import FinanceTest, Instrument, Fund, Position


class DeleteTest(FinanceTest):
    
    def setUp(self):
        self.register()
        self.data.create()
        self.instruments = Instrument.objects.all()
                        
    def testDelete(self):
        for inst in self.instruments:
            inst.delete()
            
    def testDeleteOneByOneTransaction(self):
        session = Instrument.objects.session()
        with session.begin():
            for inst in self.instruments:
                session.delete(inst)
        
    def testDeleteTransaction(self):
        Instruments.objects.query().delete()
