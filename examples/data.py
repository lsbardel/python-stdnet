from datetime import date
from random import randint

from stdnet import test
from stdnet.utils import populate, zip, iteritems

from .models import Instrument, Fund, Position


CCYS_TYPES = ['EUR','GBP','AUD','USD','CHF','JPY']
INSTS_TYPES = ['equity','bond','future','cash','option','bond option']


class key_data(object):
    sizes = {'tiny': 10,
             'small': 100,
             'normal': 1000,
             'big': 10000,
             'huge': 1000000}
    
    def __init__(self, size, sizes = None, **kwargs):
        self.sizes = sizes or self.sizes
        self.size = self.sizes[size]
        self.generate(**kwargs)
        
    def generate(self, min_len = 10, max_len = 20, **kwargs):
        self.keys = populate('string', self.size, min_len = min_len,
                             max_len = max_len)
        self.values = populate('string', self.size, min_len = min_len+10,
                               max_len = max_len+20)
        
    def mapping(self, prefix = ''):
        for k,v in zip(self.keys,self.values):
            yield prefix+k,v
            

class tsdata(key_data):
    
    def generate(self, fields = None, datatype = 'float', 
                 start = None, end = None, **kwargs):
        fields = fields or ('data',)
        start = start or date(1997,1,1)
        end = end or date.today()
        self.dates = populate('date', self.size, start = start, end = end)
        self.fields = {}
        for field in fields:
            self.fields[field] = populate(datatype, self.size)
        self.values = []
        for i,dt in enumerate(self.dates):
            vals = dict(((f,v[i]) for f,v in iteritems(self.fields)))
            self.values.append((dt,vals))


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
        self.dates =  populate('date',num_dates,start=date(2009,6,1),
                               end=date(2010,6,6))
    
    def create(self, test, use_transaction = True):
        session = test.session()
        test.assertEqual(session.query(Instrument).count(),0)
        if use_transaction:
            with session.begin():
                for name,ccy in zip(self.fund_names,self.fund_ccys):
                    session.add(Fund(name = name, ccy = ccy))
                for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                        self.inst_ccys):
                    session.add(Instrument(name = name, type = typ, ccy = ccy))
        else:
            test.register()
            for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                    self.inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save()     
            for name,ccy in zip(self.fund_names,self.fund_ccys):
                Fund(name = name, ccy = ccy).save()
                
        self.num_insts = session.query(Instrument).count()
        self.num_funds = session.query(Fund).count()
        test.assertEqual(self.num_insts,len(self.inst_names))
        test.assertEqual(self.num_funds,len(self.fund_names))
        return session
    
    def makePositions(self, test, use_transaction = True):
        session = self.create(test, use_transaction)
        instruments = session.query(Instrument).all()
        if use_transaction:
            with session.begin():
                for f in session.query(Fund):
                    insts = populate('choice', self.pos_len,
                                     choice_from = instruments)
                    for dt in self.dates:
                        for inst in insts:
                            session.add(Position(instrument = inst,
                                                 dt = dt,
                                                 fund = f,
                                                 size = randint(-100000,
                                                                 100000)))
        else:
            for f in Fund.objects.query(Fund):
                insts = populate('choice', self.pos_len,
                                choice_from = instruments)
                for dt in self.dates:
                    for inst in insts:
                        Position(instrument = inst, dt = dt, fund = f).save()
                        
        self.num_pos = session.query(Position).count()
        return session
        
    
class FinanceTest(test.TestCase):
    '''A class for testing the Finance application example. It can be run
with different sizes by passing the'''
    models = (Instrument, Fund, Position)
    
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data = finance_data(size = size)
        
        
    