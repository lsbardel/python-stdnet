from stdnet import odm
from stdnet.utils import test

from examples.models import Role, Profile
from examples.m2m import Composite, Element, CompositeElement

    
class TestManyToManyBase(test.CleanTestCase):
    models = (Role, Profile)
        
    def addsome(self):
        session = self.session()
        with session.begin():
            profile = session.add(Profile())
            profile2 = session.add(Profile())
            profile3 = session.add(Profile())
            role1 = session.add(Role(name='admin'))
            role2 = session.add(Role(name='coder'))
        with session.begin():
            profile.roles.add(role1)
            profile.roles.add(role2)
        # Check role    
        t1 = role1.profiles.throughquery().all()
        t2 = role2.profiles.throughquery().all()
        self.assertEqual(len(t1), 1)
        self.assertEqual(len(t2), 1)
        self.assertEqual(t1[0].role, role1)
        self.assertEqual(t2[0].role, role2)
        #
        p1 = role1.profiles.query().all()
        p2 = role2.profiles.query().all()
        self.assertEqual(len(p1), 1)
        self.assertEqual(len(p2), 1)
        self.assertEqual(p1[0], profile)
        self.assertEqual(p2[0], profile)
        #
        # Check profile
        t1 = profile.roles.throughquery().all()
        self.assertEqual(len(t1),2)
        self.assertEqual(t1[0].profile,profile)
        self.assertEqual(t1[1].profile,profile)
        #
        r = profile.roles.query().all()
        self.assertEqual(len(r),2)
        self.assertEqual(set(r),set((role1,role2)))
        

class TestManyToMany(TestManyToManyBase):
    
    def testMeta(self):
        roles = Profile.roles
        self.assertEqual(roles.model._meta.name,'profile_role')
        self.assertEqual(roles.relmodel,Profile)
        self.assertEqual(roles.name_relmodel,'profile')
        self.assertEqual(roles.formodel,Role)
        profiles = Role.profiles
        self.assertEqual(profiles.model._meta.name,'profile_role')
        self.assertEqual(profiles.relmodel,Role)
        self.assertEqual(profiles.formodel,Profile)
        self.assertEqual(profiles.name_relmodel,'role')
        #
        through = roles.model
        self.assertEqual(through, profiles.model)
        self.assertEqual(len(through._meta.dfields),3)
        
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
        session = self.session()
        profile = session.query(Profile).get(id = 1)
        self.assertEqual(profile.roles.query().count(),2)
        profile.delete()
        roles = session.query(Role).all()
        self.assertTrue(roles)
        for role in roles:
            self.assertEqual(role.profiles.query().count(),0)
    
    def testDelete2(self):
        self.addsome()
        session = self.session()
        roles = session.query(Role)
        self.assertEqual(roles.count(),2)
        roles.delete()
        self.assertEqual(session.query(Role).count(),0)
        profile = session.query(Profile).get(id = 1)
        self.assertEqual(profile.roles.query().count(),0)
        profile.delete()
        
    def testRemove(self):
        session = self.session()
        with session.begin():
            p1 = session.add(Profile())
            p2 = session.add(Profile())
        role, created = session.get_or_create(Role, name='admin')
        if created:
            role.save()
        self.assertTrue(role.id)
        p1.roles.add(role)
        p2.roles.add(role)
        profiles = role.profiles.query()
        self.assertEqual(profiles.count(),2)
        p2.roles.add(role)
        profiles = role.profiles.query()
        self.assertEqual(profiles.count(),2)
        p2.roles.remove(role)
        profiles = role.profiles.query()
        self.assertEqual(profiles.count(),1)
        p1.roles.remove(role)
        profiles = role.profiles.query()
        self.assertEqual(profiles.count(),0)
        
        
class TestRegisteredThroughModel(TestManyToManyBase):
    
    def setUp(self):
        self.register()
        
    def testMeta(self):
        through = Profile.roles.model
        self.assertEqual(through.objects.backend, Profile.objects.backend)
        
    def testQueryOnThroughModel(self):
        self.addsome()
        query = Profile.roles.query()
        self.assertEqual(query.model, Role)
        self.assertEqual(query.count(), 2)
        

class TestManyToManyThrough(test.TestCase):
    models = (Composite, Element)
    
    def testMetaComposite(self):
        meta = Composite._meta
        m2m = None
        for field in meta.fields:
            if field.name == 'elements':
                m2m = field
        self.assertTrue(isinstance(m2m, odm.ManyToManyField))
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