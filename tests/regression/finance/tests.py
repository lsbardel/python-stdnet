import datetime
import logging
from random import randint

from stdnet import test
from stdnet.utils import populate, zip
from stdnet.exceptions import QuerySetError

from examples.models import Instrument, Fund, Position, PortfolioView, UserDefaultView


INST_LEN    = 100
FUND_LEN    = 10
POS_LEN     = 30
NUM_USERS   = 10
NUM_DATES   = 2

ccys_types  = ['EUR','GBP','AUD','USD','CHF','JPY']
insts_types = ['equity','bond','future','cash','option','bond option']

def finance_data(inst_len = INST_LEN, fund_len = FUND_LEN,
                 num_dates = NUM_DATES):
    return (
            populate('string',inst_len, min_len = 5, max_len = 20),
            populate('choice',inst_len, choice_from = insts_types),
            populate('choice',inst_len, choice_from = ccys_types),
            populate('string',fund_len, min_len = 5, max_len = 20),
            populate('choice',fund_len, choice_from = ccys_types),
            populate('date',num_dates,start=datetime.date(2009,6,1),
                     end=datetime.date(2010,6,6))
            )
    

inst_names,inst_types,inst_ccys,fund_names,fund_ccys,dates = finance_data() 

users      = populate('string', NUM_USERS, min_len = 8, max_len = 14)
view_names = populate('string', 4*FUND_LEN, min_len = 10, max_len = 20)


class BaseFinance(test.TestCase):
    
    def setUp(self):
        '''Create Instruments and Funds commiting at the end for speed'''
        orm = self.orm
        orm.register(Instrument)
        orm.register(Fund)
        orm.register(Position)
        orm.register(PortfolioView)
        orm.register(UserDefaultView)
        with Instrument.transaction() as t:
            for name,typ,ccy in zip(inst_names,inst_types,inst_ccys):
                Instrument(name = name, type = typ, ccy = ccy).save(t)
        with Fund.transaction() as t:        
            for name,ccy in zip(fund_names,fund_ccys):
                Fund(name = name, ccy = ccy).save(t)
        
    def makePositions(self):
        '''Create Positions objects which hold foreign key to instruments and funds'''
        instruments = Instrument.objects.all()
        n = 0
        with Position.transaction() as t:
            for f in Fund.objects.all():
                insts = populate('choice',POS_LEN,choice_from = instruments)
                for dt in dates:
                    for inst in insts:
                        n += 1
                        Position(instrument = inst, dt = dt, fund = f).save(t)
        return n


class TestFinanceApplication(BaseFinance):
        
    def testGetObject(self):
        '''Test get method for id and unique field'''
        obj = Instrument.objects.get(id = 1)
        self.assertEqual(obj.id,1)
        self.assertTrue(obj.name)
        obj2 = Instrument.objects.get(name = obj.name)
        self.assertEqual(obj,obj2)
        
    def testLen(self):
        '''Simply test len of objects greater than zero'''
        objs = Instrument.objects.all()
        self.assertTrue(len(objs)>0)
    
    def testFilter(self):
        '''Test filtering on a model without foreignkeys'''
        instget = lambda : Instrument.objects.get(type = 'equity')
        self.assertRaises(QuerySetError,instget)
        tot = 0
        for t in insts_types:
            fs = Instrument.objects.filter(type = t)
            N  = fs.count()
            count = {}
            for f in fs:
                count[f.ccy] = count.get(f.ccy,0) + 1
            for c in ccys_types:
                x = count.get(c,0)
                objs = fs.filter(ccy = c)
                y = 0
                for obj in objs:
                    y += 1
                    tot += 1
                    self.assertEqual(obj.type,t)
                    self.assertEqual(obj.ccy,c)
                self.assertEqual(x,y)
        all = Instrument.objects.all()
        self.assertEqual(tot,len(all))
        
    def testValidation(self):
        pos = Position()
        self.assertFalse(pos.is_valid())
        self.assertEqual(len(pos.errors),3)
        self.assertEqual(len(pos.cleaned_data),1)
        self.assertTrue('size' in pos.cleaned_data)
        
    def testForeignKey(self):
        '''Test filtering with foreignkeys'''
        self.makePositions()
        #
        positions = Position.objects.all()
        for p in positions:
            self.assertTrue(isinstance(p.instrument,Instrument))
            self.assertTrue(isinstance(p.fund,Fund))
            pos = Position.objects.filter(instrument = p.instrument,
                                          fund = p.fund)
            found = 0
            for po in pos:
                if po == p:
                    found += 1
            self.assertEqual(found,1)
                
        # Testing 
        total_positions = len(positions)
        totp = 0
        for instrument in Instrument.objects.all():
            pos  = list(instrument.positions.all())
            for p in pos:
                self.assertTrue(isinstance(p,Position))
                self.assertEqual(p.instrument,instrument)
            totp += len(pos)
        
        self.assertEqual(total_positions,totp)
        
    def testRelatedManagerFilter(self):
        self.makePositions()
        instruments = Instrument.objects.all()
        for instrument in instruments:
            positions = instrument.positions.all()
            funds = {}
            flist = []
            for pos in positions:
                fund = pos.fund
                n    = funds.get(fund.id,0) + 1
                funds[fund.id] = n
                if n == 1:
                    flist.append(fund)
            for fund in flist:
                positions = instrument.positions.filter(fund = fund)
                self.assertEqual(len(positions),funds[fund.id])
            
        
    def testDeleteSimple(self):
        '''Test delete on models without related models'''
        instruments = Instrument.objects.all()
        funds = Fund.objects.all()
        self.assertTrue(instruments.count())
        self.assertTrue(funds.count())
        instruments.delete()
        funds.delete()
        self.assertFalse(Instrument.objects.all().count())
        self.assertFalse(Fund.objects.all().count())
        
    def testDelete(self):
        '''Test delete on models with related models'''
        # Create Positions which hold foreign keys to Instruments
        self.makePositions()
        instruments = Instrument.objects.all()
        self.assertTrue(instruments.count())
        self.assertTrue(Position.objects.all().count())
        instruments.delete()
        self.assertFalse(instruments.count())
        self.assertFalse(Position.objects.all().count())
        
    def __testNestedLookUp(self):
        # Create Portfolio views
        funds = Fund.objects.all()
        N     = funds.count()
        with PortfolioView.transaction() as t:
            for name in view_names:
                fund = funds[randint(0,N-1)] 
                PortfolioView(name = name, portfolio = fund).save(t)
        views = PortfolioView.objects.all()
        N = views.count()
        with UserDefaultView.transaction() as t:
            for user in users:
                for i in range(0,FUND_LEN): 
                    view = views[randint(0,N-1)]
                    user = UserDefaultView(user = user, view = view).save(t)
        #
        #Finally do the filtering
        N = 0
        for fund in funds:
            res = UserDefaultView.objects.filter(view__portfolio = fund)
            N += res.count()
            
        
         

