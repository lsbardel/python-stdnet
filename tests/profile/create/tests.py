from stdnet import test

from regression.finance.tests import Instrument, finance_data

inst_names,inst_types,inst_ccys,fund_names,fund_ccys,dates =\
finance_data(10000,1000,10)


class SimpleCreate(test.ProfileTest):
    
    def register(self):
        self.orm.register(Instrument)
        
    def run(self):
        for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
            Instrument(name = name, type = typ, ccy = ccy).save()
        

