from examples.models import Instrument, Fund, Position
from regression.finance import tests as fintests


class TestFiler(fintests.BaseFinance):
    
    def testAll(self):
        qs = Instrument.objects.all()
        self.assertTrue(qs.count() > 0)
        
    def testFilterIn(self):
        filter = Instrument.objects.filter
        eur = dict(((o.id,o) for o in filter(ccy = 'EUR')))
        usd = dict(((o.id,o) for o in filter(ccy = 'USD')))
        all = set(eur).union(set(usd))
        CCYS = ('EUR','USD')
        qs = filter(ccy__in = CCYS)
        us = set()
        for inst in qs:
            us.add(inst.id)
            self.assertTrue(inst.ccy in CCYS)
        zero = all - us
        self.assertTrue(qs)
        self.assertEqual(len(zero),0)
        
    def testDoubleFilter(self):
        qs = Instrument.objects.filter(ccy = 'EUR',type = 'future')
        for inst in qs:
            self.assertEqual(inst.ccy,'EUR')
            self.assertEqual(inst.type,'future')
            
    def testDoubleFilterIn(self):
        CCYS = ('EUR','USD')
        qs = Instrument.objects.filter(ccy__in = CCYS,type = 'future')
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
            self.assertEqual(inst.type,'future')
            
    def testDoubleInFilter(self):
        CCYS = ('EUR','USD','JPY')
        types = ('equity','bond','future')
        qs = Instrument.objects.filter(ccy__in = CCYS, type__in = types)
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
            self.assertTrue(inst.type in types)
            
    def testSimpleExcludeFilter(self):
        qs = Instrument.objects.exclude(ccy = 'JPY')
        for inst in qs:
            self.assertNotEqual(inst.ccy,'JPY')
            
    def testExcludeFilterin(self):
        CCYS = ('EUR','GBP','JPY')
        A = Instrument.objects.filter(ccy__in = CCYS)
        B = Instrument.objects.exclude(ccy__in = CCYS)
        for inst in B:
            self.assertTrue(inst.ccy not in CCYS)
        all = dict(((o.id,o) for o in A))
        all.update(dict(((o.id,o) for o in B)))
        self.assertTrue(len(all),Instrument.objects.all().count())
        
    def testDoubleExclude(self):
        CCYS = ('EUR','GBP','JPY')
        types = ('equity','bond','future')
        A = Instrument.objects.exclude(ccy__in = CCYS, type__in = types)
        for inst in A:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type not in types)
        self.assertTrue(len(A))
        
    def testExcludeAndFilter(self):
        CCYS = ('EUR','GBP')
        types = ('equity','bond','future')
        qs = Instrument.objects.exclude(ccy__in = CCYS).filter(type__in = types)
        for inst in qs:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type in types)
        self.assertTrue(qs)
        
    def testFilterIds(self):
        ids = set((1,5,10))
        qs = Instrument.objects.filter(id__in = ids)
        self.assertEqual(len(qs),3)
        cids = set((o.id for o in qs))
        self.assertEqual(cids,ids)
        
    def testChangeFilter(self):
        '''Change the value of a filter field and perform filtering to check for zero values'''
        insts = Instrument.objects.filter(ccy = 'EUR')
        for inst in insts:
            inst.ccy = 'USD'
            inst.save()
        insts = Instrument.objects.filter(ccy = 'EUR')
        self.assertEqual(insts.count(),0)
        
    def testFilterWithSpace(self):
        insts = Instrument.objects.filter(type = 'bond option')
        for inst in insts:
            self.assertEqual(inst.type,'bond option')
        self.assertTrue(inst)
