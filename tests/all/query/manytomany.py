from stdnet import odm, ManyToManyError
from stdnet.utils import test

from examples.models import Role, Profile
from examples.m2m import Composite, Element, CompositeElement

    
class TestManyToManyBase(object):
    models = (Role, Profile)
        
    def addsome(self, role1='admin', role2='coder'):
        models = self.mapper
        session = models.session()
        with session.begin() as t:
            profile = t.add(models.profile(name='p1'))
            profile2 = t.add(models.profile(name='p2'))
            profile3 = t.add(models.profile(name='p3'))
            role1 = t.add(models.role(name=role1))
            role2 = t.add(models.role(name=role2))
        yield t.on_result
        with session.begin() as t:
            pr1 = profile.roles.add(role1)
            pr2 = profile.roles.add(role2)
        yield t.on_result
        self.assertEqual(len(t.saved), 1)
        self.assertEqual(len(list(t.saved.values())[0]), 2)
        # Check role    
        t1 = yield role1.profiles.throughquery().load_related('role').all()
        t2 = yield role2.profiles.throughquery().load_related('role').all()
        self.assertEqual(len(t1), 1)
        self.assertEqual(len(t2), 1)
        self.assertEqual(t1[0].role, role1)
        self.assertEqual(t2[0].role, role2)
        #
        p1 = yield role1.profiles.query().all()
        p2 = yield role2.profiles.query().all()
        self.assertEqual(len(p1), 1)
        self.assertEqual(len(p2), 1)
        self.assertEqual(p1[0], profile)
        self.assertEqual(p2[0], profile)
        #
        # Check profile
        t1 = yield profile.roles.throughquery().all()
        self.assertEqual(len(t1), 2)
        p1, p2 = yield self.multi_async((t1[0].profile, t1[1].profile))
        self.assertEqual(p1, profile)
        self.assertEqual(p2, profile)
        #
        # Check with load_only
        t1 = yield profile.roles.throughquery().load_related('profile').all()
        self.assertEqual(len(t1), 2)
        self.assertEqual(t1[0].profile, profile)
        self.assertEqual(t1[1].profile, profile)
        #
        r = yield profile.roles.query().all()
        self.assertEqual(len(r), 2)
        self.assertEqual(set(r), set((role1,role2)))
        yield role1, role2
        

class TestManyToMany(TestManyToManyBase, test.TestCase):
    
    def test_meta(self):
        self.assertEqual(Profile._meta.manytomany, ['roles'])
        roles = Profile.roles
        self.assertEqual(roles.model._meta.name, 'profile_role')
        self.assertEqual(roles.relmodel, Profile)
        self.assertEqual(roles.name_relmodel, 'profile')
        self.assertEqual(roles.formodel, Role)
        profiles = Role.profiles
        self.assertEqual(profiles.model._meta.name, 'profile_role')
        self.assertEqual(profiles.relmodel, Role)
        self.assertEqual(profiles.formodel, Profile)
        self.assertEqual(profiles.name_relmodel, 'role')
        #
        through = roles.model
        self.assertEqual(through, profiles.model)
        self.assertEqual(len(through._meta.dfields), 3)
        
    def test_meta_instance(self):
        p = Profile()
        self.assertEqual(p.roles.formodel, Role)
        self.assertEqual(p.roles.related_instance, p)
        yield self.addsome('admin', 'coder')
        role = yield self.query(Role).get(name='admin')
        self.assertEqual(role.profiles.formodel, Profile)
        self.assertEqual(role.profiles.related_instance, role)
        
    def testQuery(self):
        yield self.addsome('bla', 'foo')
        role = yield self.query(Role).get(name='bla')
        profiles = role.profiles.query()
        self.assertEqual(profiles.model, Profile)
        self.assertEqual(profiles.session, role.session)
        
    def test_throughquery(self):
        yield self.addsome('bla2', 'foo2')
        role = yield self.query(Role).get(name='bla2')
        query = role.profiles.throughquery()
        self.assertEqual(query.model, role.profiles.model)
        self.assertEqual(query.session, role.session)
        
    def test_multiple_add(self):
        yield self.addsome('bla3', 'foo3')
        role = yield self.query(Role).get(name='bla3')
        profiles = yield role.profiles.query().all()
        self.assertEqual(len(profiles), 1)
        # lets add it again
        profile = profiles[0]
        yield role.profiles.add(profile)
        profiles = yield role.profiles.query().all()
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profile, profiles[0])
        
     
class TestManyToManyAddDelete(TestManyToManyBase, test.TestWrite):
       
    def testAdd(self):
        return self.addsome()
        
    def testDelete1(self):
        role1, role2 = yield self.addsome('bla', 'foo')
        session = self.session()
        profiles = yield role1.profiles.query().all()
        self.assertEqual(len(profiles), 1)
        profile = profiles[0]
        yield self.async.assertEqual(profile.roles.query().count(), 2)
        yield session.delete(profile)
        role1, role2 = yield session.query(Role).filter(name=('bla','foo')).all()
        yield self.async.assertEqual(role1.profiles.query().count(), 0)
        yield self.async.assertEqual(role2.profiles.query().count(), 0)
        
    def testDelete2(self):
        yield self.addsome()
        session = self.session()
        roles = session.query(Role)
        yield self.async.assertEqual(roles.count(), 2)
        yield roles.delete()
        yield self.async.assertEqual(session.query(Role).count(), 0)
        profile = yield session.query(Profile).get(id=1)
        yield self.async.assertEqual(profile.roles.query().count(), 0)
        yield session.delete(profile)
        
    def test_remove(self):
        session = self.session()
        with session.begin() as t:
            p1 = t.add(Profile(name='l1'))
            p2 = t.add(Profile(name='l2'))
        yield t.on_result
        role, created = yield session.update_or_create(Role, name='gino')
        self.assertTrue(created)
        role, created = yield session.update_or_create(Role, name='gino')
        self.assertFalse(created)
        self.assertTrue(role.id)
        with p1.session.begin() as t:
            p1.roles.add(role)
            p2.roles.add(role)
            self.assertEqual(len(t.session.dirty), 2)
        yield t.on_result
        profiles = role.profiles.query()
        yield self.async.assertEqual(profiles.count(), 2)
        # add again, this shouldn't do anything
        p2.roles.add(role)
        profiles = role.profiles.query()
        yield self.async.assertEqual(profiles.count(), 2)
        # Now remove the role
        yield p2.roles.remove(role)
        profiles = role.profiles.query()
        yield self.async.assertEqual(profiles.count(),1)
        yield p1.roles.remove(role)
        profiles = role.profiles.query()
        yield self.async.assertEqual(profiles.count(), 0)
        
        
class TestRegisteredThroughModel(TestManyToManyBase, test.TestCase):
        
    def testMeta(self):
        models = self.mapper
        through = Profile.roles.model
        self.assertTrue(through in models)
        objects = models[through]
        name = through.__name__
        self.assertEqual(name, 'profile_role')
        self.assertEqual(objects.backend, models[Profile].backend)
        self.assertEqual(objects.backend, models[Role].backend)
        self.assertEqual(through.role.field.model, through)
        self.assertEqual(through.profile.field.model, through)
        pk = through.pk()
        self.assertTrue(isinstance(pk, odm.CompositeIdField))
        self.assertEqual(pk.fields[0].relmodel, Profile)
        self.assertEqual(pk.fields[1].relmodel, Role)
        
    def test_class_add(self):
        self.assertRaises(ManyToManyError, Profile.roles.add, Role(name='foo'))
        self.assertRaises(ManyToManyError, Role.profiles.add, Profile())
    
    def test_through_query(self):
        m = self.mapper
        p1, p2, p3 = yield self.multi_async((m.profile.new(name='g1'),
                                             m.profile.new(name='g2'),
                                             m.profile.new(name='g3')))
        r1, r2 = yield self.multi_async((m.role.new(name='bla'),
                                         m.role.new(name='foo')))
        # Add a role to a profile
        pr1, pr2 = yield self.multi_async((p1.roles.add(r1), p2.roles.add(r1)))
        yield self.async.assertEqual(pr1.role, r1)
        yield self.async.assertEqual(pr2.role, r1)
        

class TestManyToManyThrough(test.TestCase):
    models = (Composite, Element, CompositeElement)
    
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
        
    def testAdd(self):
        session = self.session()
        with session.begin() as t:
            c = t.add(Composite(name='test'))
            e1 = t.add(Element(name='foo'))
            e2 = t.add(Element(name='bla'))
        yield t.on_result
        yield c.elements.add(e1, weight=1.5)
        yield c.elements.add(e2, weight=-1)
        elems = yield c.elements.throughquery().all()
        for elem in elems:
            self.assertTrue(elem.weight)