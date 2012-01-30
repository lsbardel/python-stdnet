'''Benchmark deletion of instances.'''
from stdnet import test
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
        
        
        
class SaveIdOnly(FinanceTest):
    
    def setUp(self):
        self.data.create()
        self.insts = list(Instrument.objects.all().load_only('id'))
        
    def test_SaveNoIndex(self):
        for inst in self.insts:
            inst.description = 'bla'
            inst.save()
            
    def test_SaveNoIndexTransaction(self):
        with transaction(Instrument) as t:
            for inst in self.insts:
                inst.description = 'bla'
                inst.save(t)
    
    def test_SaveIndex(self):
        for inst in self.insts:
            inst.name = 'bla'
            inst.save()
            
    def test_SaveIndexTransaction(self):
        with transaction(Instrument) as t:
            for inst in self.insts:
                inst.name = 'bla'
                inst.save(t)