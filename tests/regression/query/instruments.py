from stdnet import test

from examples.models import Instrument
from examples.data import FinanceTest


class TestFilter(FinanceTest):
    model = Instrument
    
    def setUp(self):
        self.data.create(self)
        
    def testAll(self):
        session = self.session()
        qs = session.query(self.model)
        self.assertTrue(qs.count() > 0)
    
    def testSimpleFilterId_WithRedisInternal(self):
        session = self.session()
        query = session.query(self.model)
        qs = query.filter(id = 1)
        # count so that we execute the query
        self.assertEqual(qs.count(),1)
        bq = qs.backend_query()
        self.assertTrue(bq.done)
        # test the redis internals
        if qs.backend.name == 'redis':
            rqs = qs.backend_query()
            # evalsha, expire, scard
            self.assertEqual(len(rqs.commands),3)
        self.assertEqual(qs.count(),1)
        obj = qs[0]
        self.assertEqual(obj.id,1)
        self.assertEqual(obj._loadedfields,None)
        
    def testSimpleFilter(self):
        session = self.session()
        qs = session.query(self.model).filter(ccy = 'EUR')
        self.assertTrue(qs.count() > 0)
        for i in qs:
            self.assertEqual(i.ccy,'EUR')
        
    def testFilterIn(self):
        session = self.session()
        qs = session.query(self.model)
        eur = dict(((o.id,o) for o in qs.filter(ccy = 'EUR')))
        usd = dict(((o.id,o) for o in qs.filter(ccy = 'USD')))
        all = set(eur).union(set(usd))
        CCYS = ('EUR','USD')
        qs = qs.filter(ccy__in = CCYS)
        us = set()
        for inst in qs:
            us.add(inst.id)
            self.assertTrue(inst.ccy in CCYS)
        zero = all - us
        self.assertTrue(qs)
        self.assertEqual(len(zero),0)
        
    def testDoubleFilter(self):
        session = self.session()
        qs = session.query(self.model).filter(ccy = 'EUR', type = 'future')
        for inst in qs:
            self.assertEqual(inst.ccy,'EUR')
            self.assertEqual(inst.type,'future')
            
    def testDoubleFilterIn(self):
        CCYS = ('EUR','USD')
        session = self.session()
        qs = session.query(self.model).filter(ccy__in = CCYS, type = 'future')
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
            self.assertEqual(inst.type,'future')
            
    def testDoubleInFilter(self):
        CCYS = ('EUR','USD','JPY')
        types = ('equity','bond','future')
        session = self.session()
        qs = session.query(self.model).filter(ccy__in = CCYS, type__in = types)
        for inst in qs:
            self.assertTrue(inst.ccy in CCYS)
            self.assertTrue(inst.type in types)
            
    def testSimpleExcludeFilter(self):
        session = self.session()
        qs = session.query(self.model).exclude(ccy = 'JPY')
        for inst in qs:
            self.assertNotEqual(inst.ccy,'JPY')
            
    def testExcludeFilterIn(self):
        CCYS = ('EUR','GBP','JPY')
        session = self.session()
        A = session.query(self.model).filter(ccy__in = CCYS)
        B = session.query(self.model).exclude(ccy__in = CCYS)
        for inst in B:
            self.assertTrue(inst.ccy not in CCYS)
        all = dict(((o.id,o) for o in A))
        all.update(dict(((o.id,o) for o in B)))
        self.assertTrue(len(all),session.query(self.model).count())
        
    def testDoubleExclude(self):
        CCYS = ('EUR','GBP','JPY')
        types = ('equity','bond','future')
        session = self.session()
        A = session.query(self.model).exclude(ccy__in = CCYS, type__in = types)
        for inst in A:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type not in types)
        self.assertTrue(len(A))
        
    def testExcludeAndFilter(self):
        CCYS = ('EUR','GBP')
        types = ('equity','bond','future')
        session = self.session()
        query = session.query(self.model)
        qs = query.exclude(ccy__in = CCYS).filter(type__in = types)
        for inst in qs:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type in types)
        self.assertTrue(qs)
        
    def testFilterIds(self):
        '''Simple filtering on ids.'''
        ids = set((1,5,10))
        session = self.session()
        query = session.query(self.model)
        qs = query.filter(id__in = ids)
        self.assertEqual(len(qs),3)
        cids = set((o.id for o in qs))
        self.assertEqual(cids,ids)
        
    def testFilterIdExclude(self):
        CCYS = ('EUR','GBP')
        types = ('equity','bond','future')
        session = self.session()
        query = session.query(self.model)
        qt1 = set(query.filter(type__in = types))
        qt = set((i.id for i in qt1))
        qt2 = set(query.filter(id__in = qt))
        self.assertEqual(qt1,qt2)
        #
        qt3 = set(query.exclude(id__in = qt))
        qt4 = qt2.intersection(qt3)
        self.assertFalse(qt4)
        qs1 = set(query.filter(ccy__in = CCYS).exclude(type__in = types))
        qs2 = set(query.filter(ccy__in = CCYS).exclude(id__in = qt))
        self.assertEqual(qs1,qs2)
        
    def testChangeFilter(self):
        '''Change the value of a filter field and perform filtering to
 check for zero values'''
        session = self.session()
        query = session.query(self.model)
        insts = query.filter(ccy = 'EUR')
        N = insts.count()
        with session.begin():
            for inst in insts:
                self.assertEqual(inst.ccy, 'EUR')
                inst.ccy = 'USD'
                session.add(inst)
        insts = query.filter(ccy = 'EUR')
        self.assertEqual(insts.count(),0)
        
    def testFilterWithSpace(self):
        session = self.session()
        insts = session.query(self.model).filter(type = 'bond option')
        self.assertTrue(insts)
        for inst in insts:
            self.assertEqual(inst.type,'bond option')

