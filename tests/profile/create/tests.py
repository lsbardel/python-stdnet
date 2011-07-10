from stdnet import test
from stdnet.utils import populate, zip

from regression.finance.tests import Instrument, Fund, Position, finance_data

inst_names,inst_types,inst_ccys,fund_names,fund_ccys,dates =\
finance_data(10000,
             1000,
             10)
PLEN = 4


class SimpleCreate(test.ProfileTest):
    
    def register(self):
        self.orm.register(Instrument)
        self.orm.register(Fund)
        self.orm.register(Position)
        
    def run(self):
        for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
            Instrument(name = name, type = typ, ccy = ccy).save()
        for name,ccy in zip(fund_names,fund_ccys):
            Fund(name = name, ccy = ccy).save()
        instruments = Instrument.objects.all()
        for f in Fund.objects.all():
            insts = populate('choice',PLEN,choice_from = instruments)
            for dt in dates:
                for inst in insts:
                    Position(instrument = inst, dt = dt, fund = f).save()

