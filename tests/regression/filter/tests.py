from examples.models import Instrument, Fund, Position
from regression.finance import tests as fintests


class TestFiler(fintests.BaseFinance):
    
    def testAll(self):
        qs = Instrument.objects.all()
        self.assertTrue(qs.count() > 0)
        
    def testFilterIn(self):
        CCYS = ('EUR','USD')
        qs = Instrument.objects.filter(ccy__in = CCYS)
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
        #self.assertTrue(qs.count()>0)
        
    def testChangeFilter(self):
        '''Change the value of a filter field and perform filtering to check for zero values'''
        insts = Instrument.objects.filter(ccy = 'EUR')
        for inst in insts:
            inst.ccy = 'USD'
            inst.save()
        insts = Instrument.objects.filter(ccy = 'EUR')
        self.assertTrue(insts.count(),0)
        
