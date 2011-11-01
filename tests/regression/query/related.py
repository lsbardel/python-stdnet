import datetime
import random

from stdnet import test
from stdnet.orm import query

from examples.models import Node, Role, Profile, Dictionary
from examples.data import FinanceTest, Position, Instrument, Fund

STEPS   = 10


class TestSelfForeignKey(test.TestCase):
    model = Node
        
    def create(self, N, root):
        for n in range(N):
            node = self.model(parent = root,
                              weight = random.uniform(0,1)).save()
        
    def setUp(self):
        root = self.model(weight = 1.0).save()
        for n in range(STEPS):
            node = self.model(parent = root,
                              weight = random.uniform(0,1)).save()
            self.create(random.randint(0,9), node)
            
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
        self.data.makePositions()
        inst = Instrument.objects.get(id = 1)
        fund = Fund.objects.get(id = 1)
        pos = fund.positions.exclude(instrument = inst)
        for p in pos:
            self.assertFalse(p.instrument == inst)
            self.assertEqual(p.fund,fund)
            
        
class load_related(FinanceTest):
    
    def testSelectRelated(self):
        self.data.makePositions()
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
        self.data.makePositions()
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
            
            
class TestMultiField(test.TestCase):
    model = Dictionary
    
    def setUp(self):
        m = self.model(name = 'bla').save()
        m.data['ciao'] = 'bla'
        m.data['hello'] = 'foo'
        m.data['hi'] = 'pippo'
        m.data['salut'] = 'luna'
        m.save()
        m = self.model(name = 'luca').save()
        m.data['hi'] = 'pippo'
        m.data['salut'] = 'luna'
        m.save()
        
    def testloadNotSelected(self):
        '''Get the model and check that no data-structure data
 has been loaded.'''
        cache = self.model._meta.dfields['data'].get_cache_name()
        for m in self.model.objects.all():
            data = getattr(m,cache,None)
            self.assertFalse(data)
        
    def testloadselected(self):
        '''Use load_selected to load stastructure data'''
        cache = self.model._meta.dfields['data'].get_cache_name()
        for m in self.model.objects.all().load_related():
            data = getattr(m,cache,None)
            self.assertTrue(data)
            self.assertTrue(data.cache)
        
    
class TestManyToMany(test.TestCase):
    models = (Role,Profile)
        
    def testMeta(self):
        role = Role(name='admin').save()
        profiles = role.profiles
        self.assertTrue(isinstance(profiles,query.M2MRelatedManager))
        self.assertEqual(profiles.model,Profile)
        self.assertEqual(profiles.related_instance,role)
        
    def testAdd(self):
        profile = Profile().save()
        role = Role(name='admin').save()
        profile.roles.add(role)
        profiles = role.profiles.all()
        self.assertEqual(profiles.count(),1)
        p2 = profiles.filter(id = profile.id)[0]
        self.assertEqual(p2,profile)
    
    def testRemove(self):
        p1 = Profile().save()
        p2 = Profile().save()
        role,created = Role.objects.get_or_create(name='admin')
        self.assertTrue(role.id)
        p1.roles.add(role)
        p2.roles.add(role)
        profiles = role.profiles.all()
        self.assertEqual(profiles.count(),2)
        p2.roles.add(role)
        profiles = role.profiles.all()
        self.assertEqual(profiles.count(),2)
        p2.roles.remove(role)
        profiles = role.profiles.all()
        self.assertEqual(profiles.count(),1)
        p1.roles.remove(role)
        profiles = role.profiles.all()
        self.assertEqual(profiles.count(),0)
