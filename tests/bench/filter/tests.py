from stdnet import test
from stdnet.utils import zip
from stdnet.utils import populate

from examples.models import Instrument


INST_LEN    = 10000

ccys_types  = ['EUR','GBP','AUD','USD','CHF','JPY']
insts_types = ['equity','bond','future','cash','option']

inst_names = populate('string',INST_LEN, min_len = 5, max_len = 20)
inst_types = populate('choice',INST_LEN, choice_from = insts_types)
inst_ccys  = populate('choice',INST_LEN, choice_from = ccys_types)


class FilterUnorderedCount(test.BenchMark):
    tags = ['filter']
    number = 10
    model  = Instrument
    
    def register(self):
        self.orm.register(self.model)
        
    def initialise(self):
        for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
            Instrument(name = name, type = typ, ccy = ccy).save(False)
        Instrument.commit()
        
    def __str__(self):
        return '%s (%s * %s)' % (self.__class__.__name__,self.number,INST_LEN)
    
    def run(self):
        f = Instrument.objects.filter(ccy = 'EUR')
        n = f.count()
        pass
        
        
class FilterUnorderedAll(FilterUnorderedCount):
    
    def run(self):
        f = Instrument.objects.filter(ccy = 'EUR')
        v = list(f)
        f.count()