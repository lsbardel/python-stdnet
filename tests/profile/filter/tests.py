from stdnet import test
from stdnet.utils import zip
from regression.finance.tests import Instrument, inst_names, \
                                     inst_types, inst_ccys


class SimpleFilter(test.ProfileTest):
    
    def register(self):
        self.orm.register(Instrument)
        
    def initialise(self):
        for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
            Instrument(name = name, type = typ, ccy = ccy).save(False)
        Instrument.commit()
        
    def run(self):
        eur = list(Instrument.objects.filter(ccy = 'EUR'))
        usd = list(Instrument.objects.filter(ccy = 'USD'))
        usdjpy = list(Instrument.objects.filter(ccy__in = ('JPY','USD')))
    
