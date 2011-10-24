import datetime

from stdnet import transaction, test
from stdnet.utils import populate, zip

from .models import Instrument, Fund, Position

CCYS_TYPES = ['EUR','GBP','AUD','USD','CHF','JPY']
INSTS_TYPES = ['equity','bond','future','cash','option','bond option']


class finance_data(object):
    sizes = {'tiny': (20,3,10,1),
             'small': (100,10,30,2),
             'normal': (500,20,100,3),
             'big': (2000,30,500,5),
             'huge': (10000,100,1000,10)}
    
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
    
    def create(self, use_transaction = True):
        with transaction(Instrument,Fund) as t:
            t = t if use_transaction else None
            for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                    self.inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save(t)     
            for name,ccy in zip(self.fund_names,self.fund_ccys):
                Fund(name = name, ccy = ccy).save(t)
        self.num_insts = Instrument.objects.all().count()
        self.num_funds = Fund.objects.all().count()
    
    def makePositions(self, use_transaction = True):
        self.create()
        instruments = Instrument.objects.all()
        with transaction(Position) as t:
            t = t if use_transaction else None
            for f in Fund.objects.all():
                insts = populate('choice', self.pos_len,
                                 choice_from = instruments)
                for dt in self.dates:
                    for inst in insts:
                        Position(instrument = inst, dt = dt, fund = f).save(t)
        self.num_pos = Position.objects.all().count()
        
    
class FinanceTest(test.TestCase):
    models = (Instrument, Fund, Position)
    
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data = finance_data(size = size)
        
    