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
    
    def testIncr(self):
        session = self.session()
        a = session.add(self.structure())
        self.assertEqual(a.incr(),1)
        self.assertEqual(a.incr(),2)
        self.assertEqual(a.incr(3),5)
        self.assertEqual(a.incr(-7),-2)
        
    