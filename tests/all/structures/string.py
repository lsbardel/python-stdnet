import os
from datetime import date

from stdnet import odm, InvalidTransaction
from stdnet.utils import test, encoders, zip
from stdnet.utils.populate import populate

from .base import StructMixin
        

class TestString(StructMixin, test.TestCase):
    structure = odm.String
    name = 'string'
    
    def create_one(self):
        a = self.structure()
        a.push_back('this is a test')
        return a
    
    def test_incr(self):
        a = self.empty()
        a.session.add(a)
        yield self.async.assertEqual(a.incr(), 1)
        yield self.async.assertEqual(a.incr(), 2)
        yield self.async.assertEqual(a.incr(3), 5)
        yield self.async.assertEqual(a.incr(-7), -2)
        
    