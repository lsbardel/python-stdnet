import datetime
import random

from stdnet import test

from examples.models import Node, Role, Profile

STEPS   = 10

class TestSelfForeignKey(test.TestCase):
        
    def create(self, N, root):
        for n in range(N):
            node = Node(parent = root, weight = random.uniform(0,1)).save()
            
    def setUp(self):
        self.orm.register(Node)
        root = Node(weight = 1.0).save()
        for n in range(STEPS):
            node = Node(parent = root, weight = random.uniform(0,1)).save()
            self.create(random.randint(0,9), node)
            
    def unregister(self):
        self.orm.unregister(Node)
    
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
            

class TestManyToMany(test.TestCase):
    
    def setUp(self):
        self.orm.register(Role)
        self.orm.register(Profile)
        
    def unregister(self):
        self.orm.unregister(Role)
        self.orm.unregister(Profile)
        
    def testAdd(self):
        profile = Profile().save()
        role,created = Role.objects.get_or_create(name='admin')
        self.assertTrue(role.id)
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
