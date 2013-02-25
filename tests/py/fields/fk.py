import stdnet
from stdnet import odm, FieldError
from stdnet.utils import test

from examples.models import Person, Group 


class fkmeta(test.TestCase):
    models = (Person, Group)
    
    @classmethod
    def setUpClass(cls):
        super(fkmeta, cls).setUpClass()
        session = cls.session()
        with session.begin():
            session.add(Group(name='bla'))
        g = session.query(Group).get(name='bla')
        with session.begin():
            session.add(Person(name='foo', group=g))
        
    @classmethod
    def tearDownClass(cls):
        yield cls.clear_all()
        
    def testSimple(self):
        session = self.session()
        query = session.query(Person)
        self.assertEqual(query.count(), 1)
        p = query.get(name='foo')
        self.assertTrue(p.group_id)
        p.group = None
        self.assertEqual(p.group_id, None)
        
    def testOldRelatedNone(self):
        self.register()
        p = Person.objects.get(name='foo')
        g = p.group
        self.assertTrue(p)
        p.group = None
        self.assertEqual(p.group, None)
        self.assertEqual(p.group_id, None)
        self.assertRaises(stdnet.FieldValueError,p.save)
        
    def testCoverage(self):
        self.assertRaises(FieldError, odm.ForeignKey, None)
        
        