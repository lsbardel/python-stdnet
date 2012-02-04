import datetime
import random

from stdnet import test
from stdnet.orm import query

from examples.models import Node, Role, Profile, Dictionary
from examples.data import FinanceTest, Position, Instrument, Fund


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
        root = self.model(weight = 1.0).save()
        self.create(root, nesting = self.nesting)
            
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
        root = query.get(parent = None)
        qs = query.filter(parent = root)
        qs.delete()
        self.assertEqual(query.count(),1)
        self.assertEqual(query[0],root)


class TestRealtedQuery(FinanceTest):
    
    def testRelatedFilter(self):
        self.data.makePositions(self)
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
        self.data.makePositions(self)
        session = self.session()
        query = session.query(Position)
        peur = query.exclude(instrument__ccy = 'EUR')
        self.assertTrue(peur.count())
        for p in peur:
            self.assertNotEqual(p.instrument.ccy,'EUR')
    
    
class TestRelatedManager(FinanceTest):

    def testSimple(self):
        self.data.makePositions(self)
        session = self.session()
        inst = session.query(Instrument).get(id = 1)
        fund = session.query(Fund).get(id = 1)
        positions1 = session.query(Position).filter(fund = fund)
        positions = fund.positions.query()
        self.assertTrue(positions)
        for p in positions:
            self.assertEqual(p.fund,fund)
        self.assertEqual(set(positions1),set(positions))
                    
    def testExclude(self):
        self.data.makePositions(self)
        session = self.session()
        inst = session.query(Instrument).get(id = 1)
        fund = session.query(Fund).get(id = 1)
        pos = fund.positions.exclude(instrument = inst)
        for p in pos:
            self.assertFalse(p.instrument == inst)
            self.assertEqual(p.fund,fund)
        

