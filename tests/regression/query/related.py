import datetime
import random

from stdnet import test
from stdnet.orm import query

from examples.models import Node, Role, Profile, Dictionary
from examples.data import FinanceTest, Position, Instrument, Fund

STEPS   = 10


class TestSelfForeignKey(test.TestCase):
    '''The Node model is used only in this test class and should be used only
in this test class so that we can use the manager in a parallel test suite.'''
    model = Node
    nesting = 2
        
    def create(self, root, nesting):
        if nesting:
            N = random.randint(0,9)
            for n in range(N):
                node = self.model(parent = root,
                                  weight = random.uniform(0,1)).save()
                self.create(node, nesting-1)
        
    def setUp(self):
        self.register()
        root = self.model(weight = 1.0).save()
        self.create(root, nesting = self.nesting)
            
    def testMeta(self):
        for n in Node.objects.all():
            if n.parent_id:
                self.assertTrue(isinstance(n.parent,self.model))
    
    def testRelatedCache(self):
        for n in Node.objects.all():
            pcache = n._meta.dfields['parent'].get_cache_name()
            self.assertFalse(hasattr(n,pcache))
            p = n.parent
            if p:
                self.assertEqual(getattr(n,pcache),p)
                
    def testSelfRelated(self):
        root = Node.objects.filter(parent = None)
        self.assertEqual(len(root),1)
        root = root[0]
        children = list(root.children.all())
        self.assertEqual(len(children),STEPS)
        for child in children:
            self.assertEqual(child.parent,root)


class TestRelatedManager(FinanceTest):
        
    def testExclude(self):
        self.data.makePositions(self)
        session = self.session()
        inst = session.query(Instrument).get(id = 1)
        fund = session.query(Fund).get(id = 1)
        pos = fund.positions.exclude(instrument = inst)
        for p in pos:
            self.assertFalse(p.instrument == inst)
            self.assertEqual(p.fund,fund)
        

class load_related(FinanceTest):
    
    def testSelectRelated(self):
        self.data.makePositions(self)
        pos = Position.objects.all().load_related()
        self.assertTrue(pos._select_related)
        self.assertTrue(len(pos._select_related),2)
        for p in pos:
            for f in pos._select_related:
                cache = f.get_cache_name()
                val = getattr(p,cache,None)
                self.assertTrue(val)
                self.assertTrue(isinstance(val,f.relmodel))
                id = getattr(p,f.attname)
                self.assertEqual(id,val.id)
        
    def testSelectRelatedSingle(self):
        self.data.makePositions(self)
        pos = Position.objects.all().load_related('instrument')
        self.assertTrue(pos._select_related)
        self.assertTrue(len(pos._select_related),1)
        fund = Position._meta.dfields['fund']
        inst = Position._meta.dfields['instrument']
        pos = list(pos)
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            cache = fund.get_cache_name()
            val = getattr(p,cache,None)
            self.assertFalse(val)
            
