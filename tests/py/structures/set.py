from stdnet import odm
from stdnet.utils import test

from .base import StructMixin


class TestSet(StructMixin, test.TestCase):
    structure = odm.Set
    name = 'set'
    
    def create_one(self):
        s = self.structure()
        s.update((1,2,3,4,5,5))
        return s
            
    def testSimpleUpdate(self):
        # Typical usage. Add a set to a session
        session = self.session()
        s = session.add(odm.Set())
        # if not id provided, an id is created
        self.assertTrue(s.id)
        self.assertEqual(s.session, session)
        self.assertEqual(s.instance, None)
        self.assertEqual(s.size(),0)
        # this add and commit to the backend server
        s.add(8)
        self.assertEqual(s.size(),1)
        self.assertTrue(s.state().persistent)
        s.update((1,2,3,4,5,5))
        self.assertEqual(s.size(),6)
        
    def testUpdateDelete(self):
        session = self.session()
        with session.begin():
            s = session.add(odm.Set())
            s.update((1,2,3,4,5,5))
            s.discard(2)
            s.discard(67)
            s.remove(4)
            s.remove(46)
            s.difference_update((1,56,89))
        self.assertEqual(s.size(),2)
        with session.begin():
            s.difference_update((3,5,6,7))
        self.assertEqual(s.size(),0)
        
