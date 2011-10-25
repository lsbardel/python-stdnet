'''Benchmark deletion of instances.'''
from stdnet import test, transaction
from stdnet.utils import populate, zip

from examples.data import FinanceTest, Instrument, Fund, Position


class Load(FinanceTest):
    
    def setUp(self):
        self.data.create()
                        
    def testLoadOnlyId(self):
        inst = list(Instrument.objects.all().load_only('id'))
            
    def testLoadOnlyName(self):
        inst = list(Instrument.objects.all().load_only('name'))
    
    def testLoadAll(self):
        inst = list(Instrument.objects.all())
        
        
        
class Save(FinanceTest):
    
    def setUp(self):
        self.data.create()
        self.insts = list(Instrument.objects.all().load_only('id'))
        
    def testSave(self):
        for inst in self.insts:
            inst.save()
            
    def testSaveTransaction(self):
        with transaction(Instrument) as t:
            for inst in self.insts:
                inst.save(t)