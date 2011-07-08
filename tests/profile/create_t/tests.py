from stdnet import test
from stdnet.utils import zip
from profile.create.tests import Instrument, inst_names, \
                                  inst_types, inst_ccys

class SimpleCreate(test.ProfileTest):
    
    def register(self):
        self.orm.register(Instrument)
        
    def run(self):
        with Instrument.transaction() as t:
            for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save(t)
        

