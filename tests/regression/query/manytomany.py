from stdnet import test, orm
from stdnet.orm import related

from examples.models import Role, Profile
from examples.m2m import Composite, Element, CompositeElement

    
class TestManyToMany(test.TestCase):
    models = (Role, Profile)
    
    def addsome(self):
        session = self.session()
        with session.begin():
            profile = session.add(Profile())
            role1 = session.add(Role(name='admin'))
            role2 = session.add(Role(name='coder'))
            
        with session.begin():
            profile.roles.add(role1)
            profile.roles.add(role2)
            
        self.assertEqual(role1.profiles.query().count(),1)
        self.assertEqual(role2.profiles.query().count(),1)
        
        p2 = role1.profiles.all()[0]
        self.assertEqual(p2.profile,profile)
        
    def testMeta(self):
        manager = Profile.roles
        self.assertEqual(manager.model._meta.name,'profile_role')
        self.assertEqual(manager.relmodel,Profile)
        self.assertEqual(manager.formodel,Role)
        manager = Role.profiles
        self.assertEqual(manager.model._meta.name,'profile_role')
        self.assertEqual(manager.relmodel,Role)
        self.assertEqual(manager.formodel,Profile)
        
    def testMetaInstance(self):
        p = Profile()
        self.assertEqual(p.roles.formodel,Role)
        self.assertEqual(p.roles.related_instance,p)
        session = self.session()
        with session.begin():
            role = session.add(Role(name='admin'))
        self.assertEqual(role.profiles.formodel,Profile)
        self.assertEqual(role.profiles.related_instance,role)
        
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


class TestManyToManyThrough(test.TestCase):
    models = (Composite, Element)
    
    def testMetaComposite(self):
        meta = Composite._meta
        m2m = None
        for field in meta.fields:
            if field.name == 'elements':
                m2m = field
        self.assertTrue(isinstance(m2m,orm.ManyToManyField))
        self.assertFalse('elements' in meta.dfields)
        self.assertEqual(m2m.through,CompositeElement)
        self.assertTrue('elements' in meta.related)
        manager = Composite.elements
        self.assertEqual(manager.model,CompositeElement)
        self.assertEqual(manager.relmodel,Composite)
        self.assertEqual(manager.formodel,Element)
        self.assertEqual(len(CompositeElement._meta.indices),2)
        
    def testMetaElement(self):
        meta = Element._meta
        self.assertTrue('composites' in meta.related)
        manager = Element.composites
        self.assertEqual(manager.model,CompositeElement)
        self.assertEqual(manager.relmodel,Element)
        self.assertEqual(manager.formodel,Composite)