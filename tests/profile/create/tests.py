from stdnet import test
from stdnet.utils import zip
from regression.finance.tests import Instrument, inst_names, \
                                     inst_types, inst_ccys

class SimpleCreate(test.ProfileTest):
    
    def register(self):
        self.orm.register(Instrument)
        
    def run(self):
        for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
            Instrument(name = name, type = typ, ccy = ccy).save(False)
        Instrument.commit()
        

