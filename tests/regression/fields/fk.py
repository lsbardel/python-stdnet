import stdnet
from stdnet import test, transaction

from examples.models import Person, Group 


class fkmeta(test.TestCase):
    models = (Person,Group)
    
    def setUp(self):
        g = Group(name = 'bla').save()
        self.p = Person(name = 'foo', group = g).save()
        
    def testSimple(self):
        p = Person.objects.get(id = 1)
        self.assertTrue(p.group_id)
        self.assertTrue(p.group_id)
        p.group = None
        self.assertEqual(p.group_id,None)
        
    def testOldRelatedNone(self):
        p = Person.objects.get(id = 1)
        self.assertTrue(p.group)
        p.group = None
        self.assertEqual(p.group,None)
        self.assertEqual(p.group_id,None)
        self.assertRaises(stdnet.FieldValueError,p.save)
