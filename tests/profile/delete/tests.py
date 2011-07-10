from stdnet import test
from stdnet.utils import populate, zip

from regression.finance.tests import Instrument, Fund, Position, finance_data

inst_names,inst_types,inst_ccys,fund_names,fund_ccys,dates =\
finance_data(10000, #number of instruments
             1000,  # number of funds
             50)    # number of dates

PLEN = 20 #Position length per fund


class DeleteTest(test.ProfileTest):
    model = Instrument
    def register(self):
        self.orm.register(Instrument)
        self.orm.register(Fund)
        self.orm.register(Position)
        with Instrument.transaction() as t:
            for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save(t)
        with Fund.transaction() as t:        
            for name,ccy in zip(fund_names,fund_ccys):
                Fund(name = name, ccy = ccy).save(t)
        instruments = Instrument.objects.all()
        with Position.transaction() as t:
            for f in Fund.objects.all():
                insts = populate('choice',PLEN,choice_from = instruments)
                for dt in dates:
                    for inst in insts:
                        Position(instrument = inst, dt = dt, fund = f).save(t)
                        
    
    def run(self):
        for inst in self.model.objects.all():
            inst.delete()

