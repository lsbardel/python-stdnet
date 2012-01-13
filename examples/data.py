import datetime

from stdnet import test
from stdnet.utils import populate, zip

from .models import Instrument, Fund, Position


CCYS_TYPES = ['EUR','GBP','AUD','USD','CHF','JPY']
INSTS_TYPES = ['equity','bond','future','cash','option','bond option']


class finance_data(object):
    sizes = {'tiny': (20,3,10,1), # positions = 20*100*3 = 30
             'small': (100,10,30,2), # positions = 20*100*3 = 600
             'normal': (500,20,100,3), # positions = 20*100*3 = 6,000
             'big': (2000,30,200,5), # positions = 30*200*5 = 30,000
             'huge': (10000,50,300,8)}# positions = 50*300*8 = 120,000
    
    def __init__(self,
                 size = 'normal',
                 insts_types = None,
                 ccys_types = None):
        inst_len, fund_len, pos_len, num_dates = self.sizes.get(size)
        insts_types = insts_types or INSTS_TYPES
        ccys_types = ccys_types or CCYS_TYPES
        self.pos_len = pos_len
        self.inst_names = populate('string',inst_len, min_len = 5, max_len = 20)
        self.inst_types = populate('choice',inst_len, choice_from = insts_types)
        self.inst_ccys = populate('choice',inst_len, choice_from = ccys_types)
        self.fund_names = populate('string',fund_len, min_len = 5, max_len = 20)
        self.fund_ccys = populate('choice',fund_len, choice_from = ccys_types)
        self.dates =  populate('date',num_dates,start=datetime.date(2009,6,1),
                         end=datetime.date(2010,6,6))
    
    def create(self, test, use_transaction = True):
        session = test.session()
        if use_transaction:
            with session.begin():
                for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                        self.inst_ccys):
                    session.add(Instrument(name = name, type = typ, ccy = ccy))     
                for name,ccy in zip(self.fund_names,self.fund_ccys):
                    session.add(Fund(name = name, ccy = ccy))
        else:
            self.register()
            for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                    self.inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save()     
            for name,ccy in zip(self.fund_names,self.fund_ccys):
                Fund(name = name, ccy = ccy).save()
                
        self.num_insts = session.query(Instrument).count()
        self.num_funds = session.query(Fund).count()
    
    def makePositions(self, test, use_transaction = True):
        self.create(test, use_transaction)
        session = test.session()
        instruments = session.query(Instrument).all()
        if use_transaction:
            with session.begin():
                for f in session.query(Fund):
                    insts = populate('choice', self.pos_len,
                                     choice_from = instruments)
                    for dt in self.dates:
                        for inst in insts:
                            session.add(Position(instrument = inst, dt = dt,
                                                 fund = f))
        else:
            for f in Fund.objects.query(Fund):
                insts = populate('choice', self.pos_len,
                                choice_from = instruments)
                for dt in self.dates:
                    for inst in insts:
                        Position(instrument = inst, dt = dt, fund = f).save()
                        
        self.num_pos = session.query(Position).count()
        
    
class FinanceTest(test.TestCase):
    '''A class for testing the Finance application example. It can be run
with different sizes by passing the'''
    models = (Instrument, Fund, Position)
    
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data = finance_data(size = size)
        
    