'''Benchmark filtering'''
from stdnet import test, transaction
from stdnet.utils import zip
from stdnet.utils import populate

from examples.models import Instrument

ccys_types  = ['EUR','GBP','AUD','USD','CHF','JPY']
insts_types = ['equity','bond','future','cash','option']


class QueryTest(test.TestModelBase):
    __number__ = 10
    model = Instrument
    sizes = {'tiny': 100,
             'small': 500,
             'normal': 1000,
             'big': 5000,
             'huge': 10000}
    
    def setUp(self):
        size = self.sizes.get(getattr(self,'test_size','normal'))
        inst_names = populate('string',size, min_len = 5, max_len = 20)
        inst_types = populate('choice',size, choice_from = insts_types)
        inst_ccys  = populate('choice',size, choice_from = ccys_types)
        with transaction(Instrument) as t:
            for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save(t)
    
    def testCount(self):
        f = Instrument.objects.filter(ccy = 'EUR')
        n = f.count()

    def testSimpleFilter(self):
        f = Instrument.objects.filter(ccy = 'EUR')
        v = list(f)
        f.count()
        
    def testInFilter(self):
        f = Instrument.objects.filter(ccy__in = ('JPY','USD'))
        v = list(f)
        f.count()
