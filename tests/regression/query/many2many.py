from stdnet import test
from stdnet.orm import related

from examples.models import Role, Profile

    
class TestManyToMany(test.TestCase):
    models = (Role, Profile)
    
    def addsome(self):
        session = self.session()
        with session.begin():
            profile = session.add(Profile())
            role1 = session.add(Role(name='admin'))
            profile.roles.add(role1)
        self.assertEqual(role1.profiles.count(),1)
        
        with session.begin():
            role2 = session(Role(name='coder'))
            profile.roles.add(role2)
        self.assertEqual(role2.profiles.count(),1)
        
        p2 = role1.profiles.filter(id = profile.id)[0]
        self.assertEqual(p2,profile)
        
    def testMeta(self):
        p = Profile()
        self.assertTrue(p.roles)
        session = self.session()
        with session.begin():
            role = session.add(Role(name='admin'))
        profiles = role.profiles
        self.assertTrue(isinstance(profiles,related.Many2ManyRelatedManager))
        self.assertEqual(profiles.model,Profile)
        self.assertEqual(profiles.related_instance,role)
        
    def testAdd(self):
        self.addsome()
        
    def testDelete1(self):
        self.addsome()
        profile = Profile.objects.get(id = 1)
        self.assertEqual(profile.roles.all().count(),2)
        profile.delete()
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
