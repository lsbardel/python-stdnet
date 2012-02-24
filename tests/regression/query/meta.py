'''Test query meta and corner cases'''
from stdnet import test

from examples.models import Instrument
from examples.data import FinanceTest


class TestMeta(FinanceTest):
    
    def setUp(self):
        self.register()
        
    def testEmpty(self):
        empty = Instrument.objects.empty()
        self.assertEqual(empty.meta,Instrument._meta)
        self.assertEqual(len(empty),0)
        self.assertEqual(empty.count(),0)
        self.assertEqual(list(empty),[])
        self.assertEqual(empty.executed,True)
        self.assertEqual(empty.construct(),empty)
        
    