from datetime import date, timedelta
from random import randint

from stdnet.utils import test, populate, zip, iteritems

from .models import Instrument, Fund, Position


CCYS_TYPES = ['EUR','GBP','AUD','USD','CHF','JPY']
INSTS_TYPES = ['equity','bond','future','cash','option','bond option']


def assertEqual(x, y):
    assert x == y, 'no equal'


class key_data(test.DataGenerator):

    def generate(self, min_len=10, max_len=20, **kwargs):
        self.keys = populate('string', self.size, min_len=min_len,
                             max_len=max_len)
        self.values = populate('string', self.size, min_len = min_len+10,
                               max_len = max_len+20)

    def mapping(self, prefix = ''):
        for k,v in zip(self.keys,self.values):
            yield prefix+k,v


class hash_data(key_data):
    sizes = {'tiny': (50,30), # fields/average field size
             'small': (300,100),
             'normal': (1000,300),
             'big': (5000,1000),
             'huge': (20000,5000)}

    def generate(self, fieldtype='string', **kwargs):
        fsize,dsize = self.size
        if fieldtype == 'date':
            self.fields = populate('date', fsize,
                              start = date(1971,12,30),
                              end = date.today())
        else:
            self.fields = populate('string', fsize, min_len = 5, max_len = 30)
        self.data = populate('string', fsize, min_len = dsize, max_len = dsize)

    def items(self):
        return zip(self.fields,self.data)


class finance_data(test.DataGenerator):
    sizes = {'tiny': (20,3,10,1), # positions = 20*100*3 = 30
             'small': (100,10,30,2), # positions = 20*100*3 = 600
             'normal': (500,20,100,3), # positions = 20*100*3 = 6,000
             'big': (2000,30,200,5), # positions = 30*200*5 = 30,000
             'huge': (10000,50,300,8)}# positions = 50*300*8 = 120,000

    def generate(self, insts_types=None, ccys_types=None, **kwargs):
        inst_len, fund_len, pos_len, num_dates = self.size
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

    def create(self, test, use_transaction=True):
        session = test.session()
        models = test.mapper
        eq = assertEqual if isinstance(test, type) else test.assertEqual
        c = yield models.instrument.query().count()
        eq(c, 0)
        if use_transaction:
            with session.begin() as t:
                for name,ccy in zip(self.fund_names,self.fund_ccys):
                    t.add(models.fund(name=name, ccy=ccy))
                for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                        self.inst_ccys):
                    t.add(models.instrument(name=name, type=typ, ccy=ccy))
            yield t.on_result
        else:
            test.register()
            for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                    self.inst_ccys):
                yield models.instrument.new(name=name, type=typ, ccy=ccy)
            for name,ccy in zip(self.fund_names,self.fund_ccys):
                yield models.fund(name=name, ccy=ccy)
        self.num_insts = yield models.instrument.query().count()
        self.num_funds = yield models.fund.query().count()
        eq(self.num_insts, len(self.inst_names))
        eq(self.num_funds, len(self.fund_names))
        yield session

    def makePositions(self, test, use_transaction=True):
        session = yield self.create(test, use_transaction)
        instruments = yield session.query(Instrument).all()
        funds = yield session.query(Fund).all()
        if use_transaction:
            with session.begin() as t:
                for f in funds:
                    insts = populate('choice', self.pos_len,
                                     choice_from = instruments)
                    for dt in self.dates:
                        for inst in insts:
                            t.add(Position(instrument=inst, dt=dt, fund=f,
                                           size=randint(-100000, 100000)))
            yield t.on_result
        else:
            for f in funds:
                insts = populate('choice', self.pos_len, choice_from=instruments)
                for dt in self.dates:
                    for inst in insts:
                        yield Position(instrument=inst, dt=dt, fund=f,
                                       size=randint(-100000, 100000)).save()
        #
        self.num_pos = yield session.query(Position).count()
        yield session


class FinanceTest(test.TestCase):
    '''A class for testing the Finance application example. It can be run
with different sizes by passing the'''
    data_cls = finance_data
    models = (Instrument, Fund, Position)
