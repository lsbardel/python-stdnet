from stdnet.utils import populate, zip

from examples.models import Instrument, Instrument2
from regression.finance import tests as fintests


class TestFilter(fintests.BaseFinance):
    model = Instrument
    
    def setUp(self):
        self.orm.register(self.model)
        super(TestFilter,self).setUp()
        
    def testAll(self):
        qs = self.model.objects.all()
        self.assertTrue(qs.count() > 0)
        
    def testFilterIn(self):
        filter = self.model.objects.filter
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
        qs = self.model.objects.filter(ccy = 'EUR',type = 'future')
        for inst in qs:
            self.assertEqual(inst.ccy,'EUR')
            self.assertEqual(inst.type,'future')
            
    def testDoubleFilterIn(self):
        CCYS = ('EUR','USD')
        qs = self.model.objects.filter(ccy__in = CCYS,type = 'future')
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
            self.assertEqual(inst.type,'future')
            
    def testDoubleInFilter(self):
        CCYS = ('EUR','USD','JPY')
        types = ('equity','bond','future')
        qs = self.model.objects.filter(ccy__in = CCYS, type__in = types)
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
            self.assertTrue(inst.type in types)
            
    def testSimpleExcludeFilter(self):
        qs = self.model.objects.exclude(ccy = 'JPY')
        for inst in qs:
            self.assertNotEqual(inst.ccy,'JPY')
            
    def testExcludeFilterin(self):
        CCYS = ('EUR','GBP','JPY')
        A = self.model.objects.filter(ccy__in = CCYS)
        B = self.model.objects.exclude(ccy__in = CCYS)
        for inst in B:
            self.assertTrue(inst.ccy not in CCYS)
        all = dict(((o.id,o) for o in A))
        all.update(dict(((o.id,o) for o in B)))
        self.assertTrue(len(all),self.model.objects.all().count())
        
    def testDoubleExclude(self):
        CCYS = ('EUR','GBP','JPY')
        types = ('equity','bond','future')
        A = self.model.objects.exclude(ccy__in = CCYS, type__in = types)
        for inst in A:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type not in types)
        self.assertTrue(len(A))
        
    def testExcludeAndFilter(self):
        CCYS = ('EUR','GBP')
        types = ('equity','bond','future')
        qs = self.model.objects.exclude(ccy__in = CCYS).filter(type__in = types)
        for inst in qs:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type in types)
        self.assertTrue(qs)
        
    def testFilterIds(self):
        ids = set((1,5,10))
        qs = self.model.objects.filter(id__in = ids)
        self.assertEqual(len(qs),3)
        self.assertTrue(qs.simple)
        cids = set((o.id for o in qs))
        self.assertEqual(cids,ids)
        
    def testFilterIdExclude(self):
        CCYS = ('EUR','GBP')
        types = ('equity','bond','future')
        qt1 = set(self.model.objects.filter(type__in = types))
        qt = set((i.id for i in qt1))
        qt2 = set(self.model.objects.filter(id__in = qt))
        self.assertEqual(qt1,qt2)
        #
        qt3 = set(self.model.objects.exclude(id__in = qt))
        qt4 = qt2.intersection(qt3)
        self.assertFalse(qt4)
        qs1 = set(self.model.objects.filter(ccy__in = CCYS).exclude(type__in = types))
        qs2 = set(self.model.objects.filter(ccy__in = CCYS).exclude(id__in = qt))
        self.assertEqual(qs1,qs2)
        
    def testChangeFilter(self):
        '''Change the value of a filter field and perform filtering to
 check for zero values'''
        insts = self.model.objects.filter(ccy = 'EUR')
        N = insts.count()
        for inst in insts:
            self.assertEqual(inst.ccy, 'EUR')
            inst.ccy = 'USD'
            inst.save()
        insts = self.model.objects.filter(ccy = 'EUR')
        self.assertEqual(insts.count(),0)
        
    def testFilterWithSpace(self):
        insts = self.model.objects.filter(type = 'bond option')
        self.assertTrue(insts)
        for inst in insts:
            self.assertEqual(inst.type,'bond option')


#class TestFilter2(TestFilter):
#    model = Instrument2
