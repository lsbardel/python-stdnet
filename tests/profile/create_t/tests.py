from stdnet import test
from stdnet.utils import zip
from profile.create.tests import Instrument, Fund, inst_names, \
                                  inst_types, fund_names, inst_ccys,\
                                  fund_ccys

class SimpleCreate(test.ProfileTest):
    
    def register(self):
        self.orm.register(Instrument)
        self.orm.register(Fund)
        
    def run(self):
        with Instrument.transaction() as t:
            for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save(t)
        with Fund.transaction() as t:        
            for name,ccy in zip(fund_names,fund_ccys):
                Fund(name = name, ccy = ccy).save(t)
        

