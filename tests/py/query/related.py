import datetime
from random import randint, uniform

from stdnet.utils import test

from examples.models import Node, Role, Profile, Dictionary
from examples.data import FinanceTest, Position, Instrument, Fund


class NodeBase(object):
    model = Node
    nesting = 2
        
    @classmethod
    def create(cls, root=None, nesting=None):
        if root is None:
            with cls.session().begin() as t:
                root = t.add(cls.model(weight=1.0))
            yield t.on_result
            yield cls.create(root, nesting=cls.nesting)
        elif nesting:
            N = randint(2,9)
            with cls.session().begin() as t:
                for n in range(N):
                    node = t.add(cls.model(parent=root, weight=uniform(0,1)))
            yield t.on_result
            yield test.multi_async((cls.create(node, nesting-1) for node in t.saved[node._meta]))
    

class TestSelfForeignKey(NodeBase, test.TestCase):
    '''The Node model is used only in this test class and should be used only
in this test class so that we can use the manager in a parallel test suite.'''
    model = Node
    nesting = 2
        
    @classmethod
    def after_setup(cls):
        return cls.create()
            
    def test_meta(self):
        all = yield self.query().load_related('parent').all()
        for n in all:
            if n.parent:
                self.assertTrue(isinstance(n.parent, self.model))
    
    def test_related_cache(self):
        all = yield self.query().all()
        pcache = self.model._meta.dfields['parent'].get_cache_name()
        for n in all:
            self.assertFalse(hasattr(n, pcache))
        yield test.multi_async((n.parent for n in all))
        for n in all:
            self.assertTrue(hasattr(n, pcache))
            self.assertEqual(getattr(n, pcache), n.parent)
                
    def test_self_related(self):
        query = self.query()
        root = yield query.get(parent=None)
        children = yield root.children.query().load_related('parent').all()
        self.assertTrue(children)
        for child in children:
            self.assertEqual(child.parent, root)
            children2 = yield child.children.query().load_related('parent').all()
            self.assertTrue(children2)
            for child2 in children2:
                self.assertEqual(child2.parent, child)
                
    def test_self_related_filter_on_self(self):
        query = self.query()
        # We should get the nodes just after the root
        root = yield query.get(parent=None)
        qs = yield query.filter(parent__parent=None).load_related('parent').all()
        self.assertTrue(qs)
        for node in qs:
            self.assertEqual(node.parent, root)


@test.sequential
class TestDeleteSelfRelated(NodeBase, test.TestCase):
    
    def setUp(self):
        return self.create()
    
    def tear_down(self):
        return self.clear_all()
    
    def testSelfRelatedDelete(self):
        yield self.query().delete()
        yield self.async.assertEqual(self.query().count(), 0)
        
    def testSelfRelatedRootDelete(self):
        qs = self.query().filter(parent=None)
        yield qs.delete()
        yield self.async.assertEqual(self.query().count(), 0)
        
    def testSelfRelatedFilterDelete(self):
        query = self.query()
        root = yield query.get(parent=None)
        qs = query.filter(parent=root)
        yield qs.delete()
        query = self.query()
        yield self.async.assertEqual(query.count(), 1)
        qs = yield query.all()
        self.assertEqual(query[0], root)


class TestRealtedQuery(FinanceTest):
    
    @classmethod
    def after_setup(cls):
        cls.data = cls.data_cls(size=cls.size)
        yield cls.data.makePositions(cls)
        
    def test_related_filter(self):
        query = self.query(Position)
        # fetch all position with EUR instruments
        instruments = self.query(Instrument).filter(ccy='EUR')
        peur1 = yield self.query(Position).filter(instrument=instruments)\
                                          .load_related('instrument').all()
        self.assertTrue(peur1)
        for p in peur1:
            self.assertEqual(p.instrument.ccy,'EUR')
        peur = self.query(Position).filter(instrument__ccy='EUR')
        qe = peur.construct()
        self.assertEqual(qe._get_field, None)
        self.assertEqual(len(qe),1)
        self.assertEqual(qe.keyword, 'set')
        peur = yield peur.all()
        self.assertEqual(set(peur), set(peur1))
            
    def test_related_exclude(self):
        query = self.query(Position)
        peur = yield query.exclude(instrument__ccy='EUR').load_related('instrument').all()
        self.assertTrue(peur)
        for p in peur:
            self.assertNotEqual(p.instrument.ccy, 'EUR')
            
    def test_load_related_model(self):
        position = yield self.query(Position).get(id=1)
        self.assertTrue(position.instrument_id)
        cache = position.get_field('instrument').get_cache_name()
        self.assertFalse(hasattr(position, cache))
        instrument = yield position.load_related_model('instrument',
                                                       load_only=('ccy',))
        self.assertTrue(isinstance(instrument, Instrument))
        self.assertEqual(instrument._loadedfields, ('ccy',))
        self.assertEqual(id(instrument), id(position.instrument))
    
    def test_related_manager(self):
        session = self.session()
        fund = yield session.query(Fund).get(id=1)
        positions1 = yield session.query(Position).filter(fund=fund).all()
        positions = yield fund.positions.query().load_related('fund').all()
        self.assertTrue(positions)
        for p in positions:
            self.assertEqual(p.fund, fund)
        self.assertEqual(set(positions1), set(positions))
                    
    def test_related_manager_exclude(self):
        inst = yield self.query().get(id=1)
        fund = yield self.query(Fund).get(id=1)
        pos = yield fund.positions.exclude(instrument=inst).load_related('instrument')\
                                                           .load_related('fund').all() 
        for p in pos:
            self.assertNotEqual(p.instrument, inst)
            self.assertEqual(p.fund, fund)
        

