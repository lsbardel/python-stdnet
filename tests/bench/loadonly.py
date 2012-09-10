'''Benchmark deletion of instances.'''
from stdnet import test
from stdnet.utils import populate, zip

from examples.data import FinanceTest, Instrument, Fund, Position


class Load(FinanceTest):
    
    def setUp(self):
        self.register()
        self.data.create(self)
                        
    def testLoadOnlyId(self):
        inst = list(Instrument.objects.query().load_only('id'))
            
    def testLoadOnlyName(self):
        inst = list(Instrument.objects.query().load_only('name'))
    
    def testLoadAll(self):
        inst = list(Instrument.objects.query())
        
        
        
class SaveIdOnly(FinanceTest):
    
    def setUp(self):
        self.register()
        self.data.create(self)
        self.insts = list(Instrument.objects.query().load_only('id'))
        
    def test_SaveNoIndex(self):
        for inst in self.insts:
            inst.description = 'bla'
            inst.save()
            
    def test_SaveNoIndexTransaction(self):
        with Instrument.objects.transaction() as t:
            for inst in self.insts:
                inst.description = 'bla'
                t.add(inst)
    
    def test_SaveIndex(self):
        i = 0
        for inst in self.insts:
            inst.name = 'blaxgfxcgsfcxgscxgdscxd%s'%i
            i += 1
            inst.save()
            
    def test_SaveIndexTransaction(self):
        i = 0
        with Instrument.objects.transaction() as t:
            for inst in self.insts:
                inst.name = 'blaxgfxcgsfcxgscxgdscxd%s'%i
                i += 1
                t.add(inst)