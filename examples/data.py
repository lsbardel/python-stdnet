from datetime import date, timedelta
from random import randint

from stdnet.utils import test, populate, zip, iteritems

from .models import Instrument, Fund, Position


CCYS_TYPES = ['EUR','GBP','AUD','USD','CHF','JPY']
INSTS_TYPES = ['equity','bond','future','cash','option','bond option']


def assertEqual(x, y):
    assert x == y, 'no equal'

class data_generator(object):
    sizes = {'tiny': 10,
             'small': 100,
             'normal': 1000,
             'big': 10000,
             'huge': 1000000}

    def __init__(self, size, sizes=None, **kwargs):
        self.sizes = sizes or self.sizes
        self.size = self.sizes[size]
        self.generate(**kwargs)

    def generate(self, **kwargs):
        raise NotImplementedError('data generation not implemented')

    def create(self, test, use_transaction=True):
        raise NotImplementedError()


class key_data(data_generator):

    def generate(self, min_len=10, max_len=20, **kwargs):
        self.keys = populate('string', self.size, min_len=min_len,
                             max_len=max_len)
        self.values = populate('string', self.size, min_len = min_len+10,
                               max_len = max_len+20)

    def mapping(self, prefix = ''):
        for k,v in zip(self.keys,self.values):
            yield prefix+k,v


class tsdata(key_data):
    '''Data generator for ColumnTS'''
    def generate(self, fields=None, datatype='float',
                 start=None, end=None, **kwargs):
        fields = fields or ('data',)
        end = end or date.today()
        if not start:
            start = end - timedelta(days = self.size)
        # random dates
        self.dates = populate('date', self.size, start=start, end=end)
        self.unique_dates = set(self.dates)
        self.fields = {}
        self.sorted_fields = {}
        for field in fields:
            self.fields[field] = populate(datatype, self.size)
            self.sorted_fields[field] = []
        self.values = []
        date_dict = {}
        for i,dt in enumerate(self.dates):
            vals = dict(((f,v[i]) for f,v in iteritems(self.fields)))
            self.values.append((dt,vals))
            date_dict[dt] = vals
        sdates = []
        for i,dt in enumerate(sorted(date_dict)):
            sdates.append(dt)
            fields = date_dict[dt]
            for field in fields:
                self.sorted_fields[field].append(fields[field])
        self.sorted_values = (sdates,self.sorted_fields)
        self.length = len(sdates)


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


class finance_data(data_generator):
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

    def create(self, test, use_transaction=True, InstrumentModel=Instrument):
        session = test.session()
        eq = assertEqual if isinstance(test, type) else test.assertEqual
        c = yield session.query(InstrumentModel).count()
        eq(c, 0)
        if use_transaction:
            with session.begin() as t:
                for name,ccy in zip(self.fund_names,self.fund_ccys):
                    t.add(Fund(name=name, ccy=ccy))
                for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                        self.inst_ccys):
                    t.add(InstrumentModel(name=name, type=typ, ccy=ccy))
            yield t.on_result
        else:
            test.register()
            for name,typ,ccy in zip(self.inst_names,self.inst_types,\
                                    self.inst_ccys):
                yield InstrumentModel(name=name, type=typ, ccy=ccy).save()
            for name,ccy in zip(self.fund_names,self.fund_ccys):
                yield Fund(name=name, ccy=ccy).save()
        self.num_insts = yield session.query(InstrumentModel).count()
        self.num_funds = yield session.query(Fund).count()
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


class DataTest(test.CleanTestCase):
    '''A class for testing the Finance application example. It can be run
with different sizes by passing the'''
    data_cls = data_generator

    @classmethod
    def setUpClass(cls):
        yield super(DataTest, cls).setUpClass()
        cls.data = yield cls.data_cls(size=cls.size)


class FinanceTest(DataTest):
    '''A class for testing the Finance application example. It can be run
with different sizes by passing the'''
    data_cls = finance_data
    models = (Instrument, Fund, Position)
