import stdnet
from stdnet import orm, test, FieldError

from examples.models import Person, Group 


class fkmeta(test.TestCase):
    models = (Person, Group)
    
    def setUp(self):
        session = self.session()
        with session.begin():
            session.add(Group(name = 'bla'))
        g = session.query(Group).get(name = 'bla')
        with session.begin():
            session.add(Person(name = 'foo', group = g))
        
    def testSimple(self):
        session = self.session()
        query = session.query(Person)
        p = query.get(id = 1)
        self.assertTrue(p.group_id)
        p.group = None
        self.assertEqual(p.group_id,None)
        
    def testOldRelatedNone(self):
        self.register()
        p = Person.objects.get(id = 1)
        self.assertTrue(p.group)
        p.group = None
        self.assertEqual(p.group,None)
        self.assertEqual(p.group_id,None)
        self.assertRaises(stdnet.FieldValueError,p.save)
        
    def testCoverage(self):
        self.assertRaises(FieldError, orm.ForeignKey, None)
        