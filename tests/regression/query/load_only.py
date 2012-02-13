from stdnet import test
from stdnet.utils import zip

from examples.models import SimpleModel, Person, Group


class LoadOnly(test.TestCase):
    model = SimpleModel
    
    def setUp(self):
        s = self.session()
        with s.begin():
            s.add(self.model(code = 'a', group = 'group1',
                             description = 'blabla'))
            s.add(self.model(code = 'b', group = 'group2',
                             description = 'blabla'))
            s.add(self.model(code = 'c', group = 'group1',
                             description = 'blabla'))
            s.add(self.model(code = 'd', group = 'group3',
                             description = 'blabla'))
            s.add(self.model(code = 'e', group = 'group1',
                             description = 'blabla'))
        
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
                self.assertFalse(hasattr(m,'description'))
                m.description = None
                s.add(m)
        # Check that description are empty
        for m in query.load_only('description'):
            self.assertFalse(m.description)
            
    def test_idonly_delete(self):
        query = self.session().query(self.model)
        query.load_only('id').delete()
        qs = query.filter(group = 'group1')
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
        original = dict(((m.id,m.group) for m in query.load_only('group')))
        self.assertEqual(query.filter(group = 'group1').count(),3)
        # save the models
        with session.begin():
            for m in query.load_only('code'):
                session.add(m)
        for m in query.load_only('group'):
            self.assertEqual(m.group,original[m.id])
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
        
        
class LoadOnlyRelated(test.TestCase):
    models = (Person, Group)
    
    def setUp(self):
        session = self.session()
        with session.begin():
            g1 = session.add(Group(name = 'bla'))
            g2 = session.add(Group(name = 'foo'))
        with session.begin():
            session.add(Person(name = 'luca', group = g1))
            session.add(Person(name = 'carl', group = g1))
            session.add(Person(name = 'bob', group = g1))
            
    def test_simple(self):
        session = self.session()
        query = session.query(Person)
        qs = query.load_only('group')
        for m in qs:
            self.assertEqual(m._loadedfields,('group',))
            self.assertFalse(hasattr(m,'name'))
            self.assertTrue(hasattr(m,'group_id'))
            self.assertTrue(m.group_id)
            self.assertTrue('id' in m._dbdata)
            self.assertEqual(m._dbdata['id'],m.id)
            g = m.group
            self.assertTrue(isinstance(g,Group))
            
            