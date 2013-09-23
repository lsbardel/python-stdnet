'''Test query.filter and query.exclude'''
from stdnet.utils import test

from examples.models import Instrument2, Fund, Position
from examples import data


class TestFilter(data.FinanceTest):
    
    @classmethod
    def after_setup(cls):
        return cls.data.create(cls)
        
    def testAll(self):
        session = self.session()
        qs = session.query(self.model)
        c = yield qs.count()
        self.assertTrue(c > 0)
    
    def testSimpleFilterId(self):
        session = self.session()
        query = session.query(self.model)
        all = yield session.query(self.model).load_only('id').all()
        qs = yield query.filter(id=all[0].id).all()
        obj = qs[0]
        self.assertEqual(obj.id, all[0].id)
        self.assertEqual(obj._loadedfields, None)
        
    def testSimpleFilter(self):
        session = self.session()
        qs = yield session.query(self.model).filter(ccy='USD').all()
        self.assertTrue(qs)
        for i in qs:
            self.assertEqual(i.ccy, 'USD')
        
    def testFilterIn(self):
        session = self.session()
        qs = session.query(self.model)
        eur = yield qs.filter(ccy='EUR').all()
        usd = yield qs.filter(ccy='USD').all()
        eur = dict(((o.id,o) for o in eur))
        usd = dict(((o.id,o) for o in usd))
        all = set(eur).union(set(usd))
        CCYS = ('EUR', 'USD')
        qs = yield qs.filter(ccy=CCYS).all()
        us = set()
        for inst in qs:
            us.add(inst.id)
            self.assertTrue(inst.ccy in CCYS)
        zero = all - us
        self.assertTrue(qs)
        self.assertEqual(len(zero),0)
        
    def testDoubleFilter(self):
        session = self.session()
        for ccy in ('EUR','USD','JPY'):
            for type in ('equity','bond','future'):
                qs = session.query(self.model).filter(ccy=ccy, type=type)
                all = yield qs.all()
                if all:
                    break
            if all:
                break
        self.assertTrue(all) 
        for inst in all:
            self.assertEqual(inst.ccy, ccy)
            self.assertEqual(inst.type, type)
            
    def testDoubleFilterIn(self):
        CCYS = ('EUR','USD')
        session = self.session()
        qs = yield session.query(self.model).filter(ccy=CCYS, type='future')
        all = yield qs.all()
        self.assertTrue(all)
        for inst in all:
            self.assertTrue(inst.ccy in CCYS)
            self.assertEqual(inst.type, 'future')
            
    def testDoubleInFilter(self):
        CCYS = ('EUR','USD','JPY')
        types = ('equity','bond','future')
        session = self.session()
        qs = session.query(self.model).filter(ccy=CCYS, type=types)
        all = yield qs.all()
        self.assertTrue(all)
        for inst in all:
            self.assertTrue(inst.ccy in CCYS)
            self.assertTrue(inst.type in types)
            
    def testSimpleExcludeFilter(self):
        session = self.session()
        qs = session.query(self.model).exclude(ccy='JPY')
        all = yield qs.all()
        self.assertTrue(all)
        for inst in all:
            self.assertNotEqual(inst.ccy, 'JPY')
            
    def testExcludeFilterIn(self):
        CCYS = ('EUR','GBP','JPY')
        session = self.session()
        A = yield session.query(self.model).filter(ccy=CCYS).all()
        B = yield session.query(self.model).exclude(ccy=CCYS).all()
        for inst in B:
            self.assertTrue(inst.ccy not in CCYS)
        all = dict(((o.id,o) for o in A))
        all.update(dict(((o.id,o) for o in B)))
        N = yield session.query(self.model).count()
        self.assertEqual(len(all), N)
        
    def testDoubleExclude(self):
        CCYS = ('EUR','GBP','JPY')
        types = ('equity','bond','future')
        session = self.session()
        qs = session.query(self.model).exclude(ccy=CCYS, type=types)
        all = yield qs.all()
        self.assertTrue(all)
        for inst in all:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type not in types)
        
    def testExcludeAndFilter(self):
        CCYS = ('EUR','GBP')
        types = ('equity','bond','future')
        session = self.session()
        query = session.query(self.model)
        qs = query.exclude(ccy=CCYS).filter(type=types)
        all = yield qs.all()
        self.assertTrue(all)
        for inst in all:
            self.assertTrue(inst.ccy not in CCYS)
            self.assertTrue(inst.type in types)
        
    def testFilterIds(self):
        '''Simple filtering on ids.'''
        session = self.session()
        all = yield session.query(self.model).load_only('id').all()
        ids = set((all[1].id, all[5].id, all[10].id))
        query = session.query(self.model)
        qs = yield query.filter(id=ids).all()
        self.assertEqual(len(qs), 3)
        cids = set((o.id for o in qs))
        self.assertEqual(cids, ids)
        
    def testFilterIdExclude(self):
        CCYS = ('EUR','GBP')
        types = ('equity','bond','future')
        session = self.session()
        query = session.query(self.model)
        qs = yield query.filter(type__in=types).all()
        qt1 = set(qs)
        qt = set((i.id for i in qt1))
        qs = yield query.filter(id__in=qt).all()
        qt2 = set(qs)
        self.assertEqual(qt1, qt2)
        #
        qs = yield query.exclude(id__in=qt).all()
        qt3 = set(qs)
        qt4 = qt2.intersection(qt3)
        self.assertFalse(qt4)
        qs1 = yield query.filter(ccy__in=CCYS).exclude(type__in=types).all()
        qs2 = yield query.filter(ccy__in=CCYS).exclude(id__in=qt).all()
        self.assertEqual(set(qs1), set(qs2))
        
    def testChangeFilter(self):
        '''Change the value of a filter field and perform filtering to
 check for zero values'''
        session = self.session()
        query = session.query(self.model)
        qs = query.filter(ccy='AUD')
        all = yield qs.all()
        self.assertTrue(all)
        with session.begin() as t:
            for inst in all:
                self.assertEqual(inst.ccy, 'AUD')
                inst.ccy = 'USD'
                t.add(inst)
        yield t.on_result
        N = yield query.filter(ccy='AUD').count()
        self.assertEqual(N, 0)
        
    def testFilterWithSpace(self):
        session = self.session()
        qs = session.query(self.model).filter(type='bond option')
        all = yield qs.all()
        self.assertTrue(all)
        for inst in all:
            self.assertEqual(inst.type,'bond option')

    def testChainedExclude(self):
        session = self.session()
        query = session.query(self.model)
        qt = query.exclude(id=(1,2,3,4)).exclude(id=(4,5,6))
        self.assertEqual(qt.eargs, {'id__in': set((1,2,3,4,5,6))})
        qt = yield qt.all()
        res = set((q.id for q in qt))
        self.assertTrue(res)
        self.assertFalse(res.intersection(set((1,2,3,4,5,6))))
        qt = query.exclude(id=3).exclude(id=4)
        self.assertEqual(qt.eargs, {'id__in': set((3,4))})
        qt = yield qt.all()
        res = set((q.id for q in qt))
        self.assertTrue(res)
        self.assertFalse(res.intersection(set((3, 4))))
        qt = yield query.exclude(id=3).exclude(id__in=(2, 4)).all()
        res = set((q.id for q in qt))
        self.assertTrue(res)
        self.assertFalse(res.intersection(set((2, 3, 4))))


class TestFilterOrdered(TestFilter):
    models = (Instrument2, Fund, Position)
    
    def test_instrument2(self):
        instrument = self.mapper.instrument
        self.assertEqual(instrument.model, Instrument2)
        self.assertEqual(instrument._meta.app_label, 'examples2')
        self.assertEqual(instrument._meta.name, 'instrument')
        self.assertEqual(instrument._meta.modelkey, 'examples2.instrument')
        self.assertEqual(instrument._meta.ordering.name, 'id')