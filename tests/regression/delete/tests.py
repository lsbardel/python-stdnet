import datetime
import logging
from random import randint

from stdnet import test
from stdnet.utils import populate, zip
from stdnet.exceptions import QuerySetError

from examples.models import Instrument, Fund, Position, Dictionary


INST_LEN    = 100
FUND_LEN    = 10
NUM_USERS   = 10
NUM_DATES   = 2

ccys_types  = ['EUR','GBP','AUD','USD','CHF','JPY']
insts_types = ['equity','bond','future','cash','option']

inst_names = populate('string',INST_LEN, min_len = 5, max_len = 20)
inst_types = populate('choice',INST_LEN, choice_from = insts_types)
inst_ccys  = populate('choice',INST_LEN, choice_from = ccys_types)

fund_names = populate('string',FUND_LEN, min_len = 5, max_len = 20)
fund_ccys  = populate('choice',FUND_LEN, choice_from = ccys_types)

users      = populate('string', NUM_USERS, min_len = 8, max_len = 14)
view_names = populate('string', 4*FUND_LEN, min_len = 10, max_len = 20)

dates = populate('date',NUM_DATES,start=datetime.date(2009,6,1),
                 end=datetime.date(2010,6,6))

DICT_LEN    = 200
dict_keys   = populate('string', DICT_LEN, min_len = 5, max_len = 20)
dict_values = populate('string', DICT_LEN, min_len = 20, max_len = 300)


class TestDeleteScalarFields(test.TestCase):
    
    def register(self):
        self.orm.register(Instrument)
        self.orm.register(Fund)
        self.orm.register(Position)
    
    def makeInstruments(self):
        with Instrument.transaction() as t:
            for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save(t)
        self.assertEqual(Instrument.objects.all().count(),INST_LEN)
        
    def makeFunds(self):
        with Fund.transaction() as t:
            for name,ccy in zip(fund_names,fund_ccys):
                Fund(name = name, ccy = ccy).save(t)
        self.assertEqual(Fund.objects.all().count(),FUND_LEN)
        
    def makePositions(self, plen = 30):
        self.makeInstruments()
        self.makeFunds()
        instruments = Instrument.objects.all()
        n = 0
        with Position.transaction() as t:
            for f in Fund.objects.all():
                insts = populate('choice',plen,choice_from = instruments)
                for dt in dates:
                    for inst in insts:
                        n += 1
                        Position(instrument = inst, dt = dt, fund = f).save(t)
        self.assertEqual(Position.objects.all().count(),n)
        return n
        
    def testFlushSimpleModel(self):
        '''Use the class method flush to remove all instances of a
 Model including filters.'''
        self.makeInstruments()
        Instrument.flush()
        self.assertEqual(Instrument.objects.all().count(),0)
        # Now we check the database if it is empty as it should
        keys = list(Instrument._meta.cursor.keys())
        self.assertEqual(len(keys),0)
        
    def testFlushRelatedModel(self):
        self.makePositions()
        Instrument.flush()
        self.assertEqual(Position.objects.all().count(),0)
        self.assertEqual(Instrument.objects.all().count(),0)
        # Now we check the database if it is empty as it should
        Fund.flush()
        keys = list(Instrument._meta.cursor.keys())
        self.assertEqual(len(keys),0)
        
    def testDeleteSimple(self):
        '''Test delete on models without related models'''
        self.makeInstruments()
        Instrument.objects.all().delete()
        self.assertEqual(Instrument.objects.all().count(),0)
        #There should be only one key in the database,
        # The one used to autoincrement the Instrument ids
        keys = list(Instrument._meta.cursor.keys())
        self.assertEqual(len(keys),1)
        self.assertEqual(keys[0],Instrument._meta.autoid())
        Instrument.flush()
        keys = list(Instrument._meta.cursor.keys())
        self.assertEqual(len(keys),0)

    def testDeleteRelatedOneByOne(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        self.makePositions(100)
        for inst in Instrument.objects.all():
            inst.delete()
        self.assertEqual(Instrument.objects.all().count(),0)
        self.assertEqual(Position.objects.all().count(),0)
                
    def testDeleteRelated(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        self.makePositions(40)
        Instrument.objects.all().delete()
        self.assertEqual(Instrument.objects.all().count(),0)
        self.assertEqual(Position.objects.all().count(),0)
        
    def __testDeleteRelatedCounting(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        NP = 20
        N = Instrument.objects.all().count() + NP
        self.makePositions(NP)
        Instrument.objects.all().delete()
        self.assertEqual(Instrument.objects.all().count(),0)
        self.assertEqual(Position.objects.all().count(),0)
        

class TestDeleteStructuredFields(test.TestCase):
    
    def setUp(self):
        '''Create Instruments and Funds commiting at the end for speed'''
        orm = self.orm
        orm.register(Dictionary)
        orm.clearall()
        Dictionary(name = 'test').save()
        Dictionary(name = 'test2').save()
        self.assertEqual(Dictionary.objects.all().count(),2)
        self.data = dict(zip(dict_keys,dict_values))
    
    def unregister(self):
        self.orm.unregister(Dictionary)
    
    def fill(self, name):
        d = Dictionary.objects.get(name = name)
        data = d.data
        d.data.update(self.data)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(self.data))
        return Dictionary.objects.get(name = name)
    
    def testSimpleFlush(self):
        Dictionary.flush()
        self.assertEqual(Dictionary.objects.all().count(),0)
        # Now we check the database if it is empty as it should
        keys = list(Dictionary._meta.cursor.keys())
        self.assertEqual(len(keys),0)
        
    def testFlushWithData(self):
        self.fill('test')
        self.fill('test2')
        Dictionary.flush()
        self.assertEqual(Dictionary.objects.all().count(),0)
        # Now we check the database if it is empty as it should
        keys = list(Dictionary._meta.cursor.keys())
        self.assertEqual(len(keys),0)
    
