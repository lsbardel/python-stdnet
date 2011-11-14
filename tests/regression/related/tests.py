import datetime
import random

from stdnet import test

from examples.models import Node, Role, Profile

STEPS   = 10


class TestSelfForeignKey(test.TestCase):
        
    def create(self, N, root):
        for n in range(N):
            node = Node(parent = root, weight = random.uniform(0,1)).save()
            
    def register(self):
        self.orm.register(Node)
        
    def unregister(self):
        self.orm.unregister(Node)
        
    def setUp(self):
        root = Node(weight = 1.0).save()
        for n in range(STEPS):
            node = Node(parent = root, weight = random.uniform(0,1)).save()
            self.create(random.randint(0,9), node)
    
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
    
    def register(self):
        self.orm.register(Role)
        self.orm.register(Profile)
        
    def unregister(self):
        self.orm.unregister(Role)
        self.orm.unregister(Profile)
        
    def addsome(self):
        profile = Profile().save()
        role1 = Role(name='admin').save()
        profile.roles.add(role1)
        self.assertEqual(role1.profiles.all().count(),1)
        
        role2 = Role(name='coder').save()
        profile.roles.add(role2)
        self.assertEqual(role2.profiles.all().count(),1)
        
        p2 = role1.profiles.filter(id = profile.id)[0]
        self.assertEqual(p2,profile)
        
    def testAdd(self):
        self.addsome()
    
    def testDelete1(self):
        self.addsome()
        profile = Profile.objects.get(id = 1)
        self.assertEqual(profile.roles.all().count(),2)
        profile.delete()
        self.assertEqual(Profile.objects.all().count(),0)
        for role in Role.objects.all():
            self.assertEqual(role.profiles.all().count(),0)
    
    def testDelete2(self):
        self.addsome()
        roles = Role.objects.all()
        self.assertEqual(roles.count(),2)
        roles.delete()
        self.assertEqual(Role.objects.all().count(),0)
        profile = Profile.objects.get(id = 1)
        self.assertEqual(profile.roles.all().count(),0)
        profile.delete()
        
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
