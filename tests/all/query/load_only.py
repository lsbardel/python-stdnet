'''test load_only and dont_load methods'''
from stdnet.utils import test, zip

from examples.models import SimpleModel, Person, Group, Statistics3


class LoadOnlyBase(test.TestCase):
    model = SimpleModel
    
    @classmethod
    def after_setup(cls):
        with cls.session().begin() as t:
            t.add(cls.model(code='a', group='group1', description='blabla'))
            t.add(cls.model(code='b', group='group2', description='blabla'))
            t.add(cls.model(code='c', group='group1', description='blabla'))
            t.add(cls.model(code='d', group='group3', description='blabla'))
            t.add(cls.model(code='e', group='group1', description='blabla'))
        return t.on_result
    
        
class LoadOnly(LoadOnlyBase):
        
    def testMeta(self):
        s = self.session()
        query = s.query(self.model)
        qs = yield query.load_only('id').all()
        for m in qs:
            self.assertEqual(m._loadedfields,())
            self.assertEqual(m.has_all_data, False)
        qs = yield query.load_only('code','group').all()
        for m in qs:
            self.assertEqual(m._loadedfields,('code','group'))
            self.assertEqual(m.has_all_data, False)
        all = yield query.all()
        for m in all:
            self.assertEqual(m._loadedfields, None)
            self.assertEqual(m.has_all_data, True)
        m = self.model(code = 'bla', group = 'foo')
        self.assertEqual(m.has_all_data, False)
        
    def test_idonly(self):
        s = self.session()
        query = s.query(self.model)
        qs = query.load_only('id')
        self.assertNotEqual(query, qs)
        self.assertEqual(qs.fields, ('id',))
        qs = yield qs.all()
        self.assertTrue(all)
        for m in qs:
            self.assertEqual(m._loadedfields,())
            self.assertEqual(tuple(m.loadedfields()),())
            self.assertFalse(hasattr(m,'code'))
            self.assertFalse(hasattr(m,'group'))
            self.assertFalse(hasattr(m,'description'))
            self.assertTrue('id' in m._dbdata)
            self.assertEqual(m._dbdata['id'], m.id)
            
    def test_idonly_None(self):
        s = self.session()
        query = s.query(self.model)
        qs = yield query.load_only('id').all()
        with s.begin():
            for m in qs:
                self.assertFalse(hasattr(m, 'description'))
                m.description = None
                s.add(m)
        # Check that description are empty
        qs = yield query.load_only('description').all()
        self.assertTrue(qs)
        for m in qs:
            self.assertFalse(m.description)
        
    def testSimple(self):
        query = self.session().query(self.model)
        qs = yield query.load_only('code').all()
        self.assertTrue(qs)
        for m in qs:
            self.assertEqual(m._loadedfields,('code',))
            self.assertTrue(m.code)
            self.assertFalse(hasattr(m,'group'))
            self.assertFalse(hasattr(m,'description'))
        qs = yield query.load_only('code','group').all()
        self.assertTrue(qs)
        for m in qs:
            self.assertEqual(m._loadedfields,('code','group'))
            self.assertTrue(m.code)
            self.assertTrue(m.group)
            self.assertFalse(hasattr(m,'description'))
            
    def testSave(self):
        session = self.session()
        query = session.query(self.model)
        qs = yield query.load_only('group').all()
        original = dict(((m.id, m.group) for m in qs))
        yield self.async.assertEqual(query.filter(group='group1').count(), 3)
        # save the models
        qs = yield query.load_only('code').all()
        with session.begin() as t:
            for m in qs:
                t.add(m)
        yield t.on_result
        qs = yield query.load_only('group').all()
        for m in qs:
            self.assertEqual(m.group, original[m.id])
        # No check indexes
        yield self.async.assertEqual(query.filter(group='group1').count(), 3)

    def test_exclude_fields(self):
        session = self.session()
        query = session.query(self.model).dont_load('description')
        self.assertEqual(query.exclude_fields, ('description',))
        qs = yield query.all()
        self.assertTrue(qs)
        for m in qs:
            self.assertFalse(hasattr(m,'description'))
        query = session.query(self.model).load_only('group')\
                                         .dont_load('description')
        qs = yield query.all()
        self.assertTrue(qs)
        for m in qs:
            self.assertEqual(m._loadedfields,('group',))
        
        
class LoadOnlyChange(LoadOnlyBase):

    def testChangeNotLoaded(self):
        '''We load an object with only one field and modify a field not
loaded. The correct behavior should be to updated the field and indexes.'''
        session = self.session()
        query = session.query(self.model)
        qs = yield query.load_only('group').all()
        original = dict(((m.id, m.group) for m in qs))
        # load only code and change the group
        qs = yield query.load_only('code').all()
        self.assertTrue(qs)
        with session.begin() as t:
            for m in qs:
                m.group = 'group4'
                t.add(m)
        yield t.on_result
        qs = query.filter(group='group1')
        yield self.async.assertEqual(qs.count(), 0)
        qs = query.filter(group='group2')
        yield self.async.assertEqual(qs.count(), 0)
        qs = query.filter(group='group3')
        yield self.async.assertEqual(qs.count(), 0)
        qs = query.filter(group='group4')
        yield self.async.assertEqual(qs.count(), 5)
        qs = yield qs.all()
        self.assertTrue(qs)
        for m in qs:
            self.assertEqual(m.group,'group4')
            
            
class LoadOnlyDelete(LoadOnlyBase):
    
    def test_idonly_delete(self):
        query = self.session().query(self.model)
        yield query.load_only('id').delete()
        qs = query.filter(group='group1')
        yield self.async.assertEqual(qs.count(), 0)
        qs = yield query.all()
        self.assertEqual(qs, [])
    
    
class LoadOnlyRelated(test.TestCase):
    models = (Person, Group)
    
    @classmethod
    def after_setup(cls):
        with cls.session().begin() as t:
            g1 = t.add(Group(name='bla', description='bla bla'))
            g2 = t.add(Group(name='foo', description='foo foo'))
        yield t.on_result
        with cls.session().begin() as t:
            t.add(Person(name='luca', group=g1))
            t.add(Person(name='carl', group=g1))
            t.add(Person(name='bob', group=g1))
        yield t.on_result
        
    def test_simple(self):
        session = self.session()
        query = session.query(Person)
        qs = yield query.load_only('group').all()
        for m in qs:
            self.assertEqual(m._loadedfields,('group',))
            self.assertFalse(hasattr(m,'name'))
            self.assertTrue(hasattr(m,'group_id'))
            self.assertTrue(m.group_id)
            self.assertTrue('id' in m._dbdata)
            self.assertEqual(m._dbdata['id'],m.id)
            g = yield m.group
            self.assertTrue(isinstance(g, Group))
            
    def testLoadForeignKeyFields(self):
        session = self.session()
        qs = yield session.query(Person).load_only('group__name').all()
        group = Person._meta.dfields['group']
        for m in qs:
            self.assertEqual(m._loadedfields, ('group',))
            self.assertFalse(hasattr(m, 'name'))
            self.assertTrue(hasattr(m, 'group_id'))
            cache_name = group.get_cache_name()
            g = getattr(m, cache_name, None)
            self.assertTrue(g)
            self.assertTrue(isinstance(g, group.relmodel))
            # And now check what is loaded with g
            self.assertEqual(g._loadedfields, ('name',))
            self.assertFalse(hasattr(g, 'description'))
            

class TestFieldReplace(test.TestCase):
    model = Statistics3
    
    @classmethod
    def after_setup(cls):
        with cls.session().begin() as t:
            t.add(cls.model(name='a',
                            data={'pv': {'': 0.5, 'mean': 1, 'std': 3.5}}))
        return t.on_result
        
    def test_load_only(self):
        session = self.session()
        query = session.query(self.model)
        s = yield query.load_only('name', 'data__pv').get(name='a')
        self.assertEqual(s.name, 'a')
        self.assertEqual(s.data__pv, 0.5)
        self.assertFalse(s.has_all_data)
        self.assertEqual(s.get_state().action, 'update')
        # Now set extra data
        s.data = {'pv': {'mean': 2}}
        with session.begin() as t:
            t.add(s)
        yield t.on_result
        s = yield query.get(name='a')
        self.assertTrue(s.has_all_data)
        self.assertEqual(s.data, {'pv': {'': 0.5, 'mean': 2, 'std': 3.5}})
        
    def test_replace(self):
        session = self.session()
        query = session.query(self.model)
        s = yield query.get(name='a')
        self.assertTrue(s.has_all_data)
        self.assertEqual(s.get_state().action, 'override')
        s.data = {'bla': {'foo': -1}}
        with session.begin() as t:
            t.add(s)
        yield t.on_result
        s = yield self.query().get(name='a')
        self.assertTrue(s.has_all_data)
        self.assertEqual(s.data, {'bla': {'foo': -1}})
        