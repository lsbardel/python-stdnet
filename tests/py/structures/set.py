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
            
    def test_update(self):
        # Typical usage. Add a set to a session
        s = self.empty()
        s.session.add(s)
        yield s.add(8)
        yield self.async.assertEqual(s.size(), 1)
        yield s.update((1,2,3,4,5,5))
        yield self.async.assertEqual(s.size(), 6)
        
    def test_update_delete(self):
        s = self.empty()
        with s.session.begin() as t:
            t.add(s)
            s.update((1,2,3,4,5,5))
            s.discard(2)
            s.discard(67)
            s.remove(4)
            s.remove(46)
            s.difference_update((1,56,89))
        yield t.on_result
        yield self.async.assertEqual(s.size(), 2)
        yield s.difference_update((3,5,6,7))
        yield self.async.assertEqual(s.size(), 0)
        
