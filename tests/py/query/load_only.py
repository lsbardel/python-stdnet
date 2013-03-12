'''test load_only and dont_load methods'''
from stdnet.utils import test, zip

from examples.models import SimpleModel, Person, Group, Statistics3


class LoadOnly(test.CleanTestCase):
    model = SimpleModel
    
    def setUp(self):
        with self.session().begin() as t:
            t.add(self.model(code='a', group='group1', description='blabla'))
            t.add(self.model(code='b', group='group2', description='blabla'))
            t.add(self.model(code='c', group='group1', description='blabla'))
            t.add(self.model(code='d', group='group3', description='blabla'))
            t.add(self.model(code='e', group='group1', description='blabla'))
        yield t.on_result
        
    def testMeta(self):
        s = self.session()
        query = s.query(self.model)
        qs = query.load_only('id')
        for m in qs:
            self.assertEqual(m._loadedfields,())
            self.assertEqual(m.has_all_data, False)
        qs = query.load_only('code','group')
        for m in qs:
            self.assertEqual(m._loadedfields,('code','group'))
            self.assertEqual(m.has_all_data, False)
        for m in query:
            self.assertEqual(m._loadedfields, None)
            self.assertEqual(m.has_all_data, True)
        m = self.model(code = 'bla', group = 'foo')
        self.assertEqual(m.has_all_data, False)
        
    def test_idonly(self):
        s = self.session()
        query = s.query(self.model)
        qs = query.load_only('id')
        self.assertNotEqual(query,qs)
        self.assertEqual(qs.fields,('id',))
        for m in qs:
            self.assertEqual(m._loadedfields,())
            self.assertEqual(tuple(m.loadedfields()),())
            self.assertFalse(hasattr(m,'code'))
            self.assertFalse(hasattr(m,'group'))
            self.assertFalse(hasattr(m,'description'))
            self.assertTrue('id' in m._dbdata)
            self.assertEqual(m._dbdata['id'],m.id)
            
    def test_idonly_None(self):
        s = self.session()
        query = s.query(self.model)
        qs = query.load_only('id')
        with s.begin():
            for m in qs:
                self.assertFalse(hasattr(m, 'description'))
                m.description = None
                s.add(m)
        # Check that description are empty
        for m in query.load_only('description'):
            self.assertFalse(m.description)
            
    def test_idonly_delete(self):
        query = self.session().query(self.model)
        query.load_only('id').delete()
        qs = query.filter(group='group1')
        self.assertEqual(qs.count(),0)
        qs = query.all()
        self.assertEqual(qs,[])
        
    def testSimple(self):
        query = self.session().query(self.model)
        for m in query.load_only('code'):
            self.assertEqual(m._loadedfields,('code',))
            self.assertTrue(m.code)
            self.assertFalse(hasattr(m,'group'))
            self.assertFalse(hasattr(m,'description'))
        for m in query.load_only('code','group'):
            self.assertEqual(m._loadedfields,('code','group'))
            self.assertTrue(m.code)
            self.assertTrue(m.group)
            self.assertFalse(hasattr(m,'description'))
            
    def testSave(self):
        session = self.session()
        query = session.query(self.model)
        original = dict(((m.id, m.group) for m in query.load_only('group')))
        self.assertEqual(query.filter(group='group1').count(),3)
        # save the models
        with session.begin():
            for m in query.load_only('code'):
                session.add(m)
        for m in query.load_only('group'):
            self.assertEqual(m.group, original[m.id])
        # No check indexes
        self.assertEqual(query.filter(group = 'group1').count(),3)
        
    def testChangeNotLoaded(self):
        '''We load an object with only one field and modify a field not
loaded. The correct behavior should be to updated the field and indexes.'''
        session = self.session()
        query = session.query(self.model)
        original = dict(((m.id,m.group) for m in query.load_only('group')))
        # load only the code and change the group
        with session.begin():
            for m in query.load_only('code'):
                m.group = 'group4'
                session.add(m)
        qs = query.filter(group = 'group1')
        self.assertEqual(qs.count(),0)
        qs = query.filter(group = 'group2')
        self.assertEqual(qs.count(),0)
        qs = query.filter(group = 'group3')
        self.assertEqual(qs.count(),0)
        qs = query.filter(group = 'group4')
        self.assertEqual(qs.count(),5)
        for m in qs:
            self.assertEqual(m.group,'group4')

    def test_exclude_fields(self):
        session = self.session()
        query = session.query(self.model).dont_load('description')
        self.assertEqual(query.exclude_fields, ('description',))
        for m in query.all():
            self.assertFalse(hasattr(m,'description'))
        query = session.query(self.model).load_only('group')\
                                         .dont_load('description')
        for m in query.all():
            self.assertEqual(m._loadedfields,('group',))
        
        
class LoadOnlyRelated(test.TestCase):
    models = (Person, Group)
    
    @classmethod
    def setUpClass(cls):
        yield super(LoadOnlyRelated, cls).setUpClass()
        session = cls.session()
        with session.begin() as t:
            g1 = t.add(Group(name='bla', description='bla bla'))
            g2 = t.add(Group(name='foo', description='foo foo'))
        yield t.on_result
        with session.begin() as t:
            t.add(Person(name='luca', group=g1))
            t.add(Person(name='carl', group=g1))
            t.add(Person(name='bob', group=g1))
        yield t.on_result
    
    @classmethod
    def tearDownClass(cls):
        yield cls.clear_all()
        
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
            g = m.group
            self.assertTrue(isinstance(g,Group))
            
    def testLoadForeignKeyFields(self):
        session = self.session()
        qs = session.query(Person).load_only('group__name')
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
            

class TestFieldReplace(test.CleanTestCase):
    model = Statistics3
    
    def setUp(self):
        s = self.session()
        with s.begin():
            s.add(self.model(name = 'a',
                             data = {'pv': {'': 0.5, 'mean': 1, 'std': 3.5}}))
        
    def test_load_only(self):
        session = self.session()
        query = session.query(self.model)
        s = query.load_only('name', 'data__pv').get(id = 1)
        self.assertEqual(s.name,'a')
        self.assertEqual(s.data__pv,0.5)
        self.assertFalse(s.has_all_data)
        s.data = {'pv':{'mean':2}}
        with session.begin() as t:
            t.add(s)
        # remove s from session so that we reloaded it
        self.assertEqual(session.expunge(s),s)
        s = query.get(id = 1)
        self.assertTrue(s.has_all_data)
        self.assertEqual(s.data, {'pv': {'': 0.5, 'mean': 2, 'std': 3.5}})
        s.data = {'bla': {'foo': -1}}
        with session.begin() as t:
            t.add(s)
        # remove s from session so that we reloaded it
        self.assertEqual(session.expunge(s),s)
        s = query.get(id = 1)
        self.assertTrue(s.has_all_data)
        self.assertEqual(s.data, {'bla': {'foo': -1}})
        