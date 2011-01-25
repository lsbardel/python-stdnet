from examples.models import Instrument, Fund, Position
from regression.finance import tests as fintests


class TestFiler(fintests.BaseFinance):
    
    def testFilterIn(self):
        CCYS = ('EUR','USD')
        qs = Instrument.objects.filter(ccy__in = CCYS)
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
        #self.assertTrue(qs.count()>0)
        
