import datetime
import random

from stdnet.utils import test

from examples.models import Node, Role, Profile, Dictionary
from examples.data import FinanceTest, Position, Instrument, Fund
from examples.data import finance_data


class TestSelfForeignKey(test.TestCase):
    '''The Node model is used only in this test class and should be used only
in this test class so that we can use the manager in a parallel test suite.'''
    model = Node
    nesting = 2
        
    def create(self, root, nesting):
        if nesting:
            N = random.randint(2,9)
            for n in range(N):
                node = self.model(parent = root,
                                  weight = random.uniform(0,1)).save()
                self.create(node, nesting-1)
        
    def setUp(self):
        self.register()
        root = yield self.model(weight = 1.0).save()
        self.create(root, nesting=self.nesting)
            
    def testMeta(self):
        session = self.session()
        for n in session.query(Node):
            if n.parent_id:
                self.assertTrue(isinstance(n.parent,self.model))
    
    def testRelatedCache(self):
        session = self.session()
        for n in session.query(Node):
            pcache = n._meta.dfields['parent'].get_cache_name()
            self.assertFalse(hasattr(n,pcache))
            p = n.parent
            if p:
                self.assertEqual(getattr(n,pcache),p)
                
    def testSelfRelated(self):
        session = self.session()
        query = session.query(Node)
        root = query.filter(parent = None)
        self.assertEqual(len(root),1)
        root = root[0]
        children = root.children.query()
        self.assertTrue(children)
        for child in children:
            self.assertEqual(child.parent,root)
            children2 = child.children.query()
            self.assertTrue(children2)
            for child2 in children2:
                self.assertEqual(child2.parent,child)
                
    def testSelfRelatedFilterOnSelf(self):
        session = self.session()
        query = session.query(Node)
        # We should get the nodes just after the root
        root = query.get(parent = None)
        qs = query.filter(parent__parent = None)
        self.assertTrue(qs.count())
        for node in qs:
            self.assertEqual(node.parent, root)

    def testSelfRelatedDelete(self):
        session = self.session()
        session.query(Node).delete()
        self.assertEqual(session.query(Node).count(),0)
        
    def testSelfRelatedRootDelete(self):
        session = self.session()
        qs = session.query(Node).filter(parent = None)
        qs.delete()
        self.assertEqual(session.query(Node).count(),0)
        
    def testSelfRelatedFilterDelete(self):
        session = self.session()
        query = session.query(Node)
        root = query.get(parent=None)
        qs = query.filter(parent=root)
        qs.delete()
        self.assertEqual(query.count(), 1)
        self.assertEqual(query[0], root)


class TestRealtedQuery(test.TestCase):
    data_cls = finance_data
    
    @classmethod
    def setUpClass(cls):
        yield super(TestRealtedQuery, cls).setUpClass()
        cls.data = cls.data_cls(size=cls.size)
        yield cls.data.makePositions(cls)
        
    @classmethod
    def tearDownClass(cls):
        yield cls.clear_all()
        
    def testRelatedFilter(self):
        session = self.session()
        query = session.query(Position)
        # fetch all position with EUR instruments
        instruments = session.query(Instrument).filter(ccy = 'EUR')
        self.assertTrue(instruments.count())
        ids = set()
        for i in instruments:
            self.assertEqual(i.ccy,'EUR')
            ids.add(i.id)
        peur1 = query.filter(instrument__in = ids)
        self.assertTrue(peur1.count())
        for p in peur1:
            self.assertTrue(p.instrument.id in ids)
            self.assertEqual(p.instrument.ccy,'EUR')
            
        peur = query.filter(instrument__ccy = 'EUR')
        qe = peur.construct()
        self.assertEqual(qe._get_field,None)
        self.assertEqual(len(qe),1)
        self.assertEqual(qe.keyword,'set')
        self.assertTrue(peur.count())
        for p in peur:
            self.assertEqual(p.instrument.ccy,'EUR')
            
    def testRelatedExclude(self):
        session = self.session()
        query = session.query(Position)
        peur = query.exclude(instrument__ccy = 'EUR')
        self.assertTrue(peur.count())
        for p in peur:
            self.assertNotEqual(p.instrument.ccy,'EUR')
            
    def test_load_related_model(self):
        session = self.session()
        query = session.query(Position)
        position = query[0]
        self.assertTrue(position.instrument_id)
        instrument = position.load_related_model('instrument',
                                                 load_only=('ccy',))
        self.assertTrue(isinstance(instrument, Instrument))
        self.assertEqual(instrument._loadedfields, ('ccy',))
        self.assertEqual(id(instrument), id(position.instrument))
    
    def test_related_manager(self):
        session = self.session()
        inst = session.query(Instrument).get(id = 1)
        fund = session.query(Fund).get(id = 1)
        positions1 = session.query(Position).filter(fund = fund)
        positions = fund.positions.query()
        self.assertTrue(positions)
        for p in positions:
            self.assertEqual(p.fund,fund)
        self.assertEqual(set(positions1),set(positions))
                    
    def test_related_manager_exclude(self):
        session = self.session()
        inst = session.query(Instrument).get(id = 1)
        fund = session.query(Fund).get(id = 1)
        pos = fund.positions.exclude(instrument = inst)
        for p in pos:
            self.assertFalse(p.instrument == inst)
            self.assertEqual(p.fund,fund)
        

