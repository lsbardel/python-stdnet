import os
from datetime import date

from stdnet import odm, InvalidTransaction
from stdnet.utils import test, encoders, zip
from stdnet.utils.populate import populate

from .base import StructMixin
    
class TestNumberArray(StructMixin, test.CleanTestCase):
    structure = odm.NumberArray
    name = 'numberarray'
    
    def create_one(self):
        a = self.structure()
        a.push_back(56).push_back(-78.6)
        return a
    
    def testSizeResize(self):
        session = self.session()
        a = session.add(self.structure())
        a.push_back(56).push_back(-78.6)
        self.assertEqual(a.size(),2)
        self.assertEqual(len(a),2)
        self.assertEqual(a.resize(10),10)
        data = list(a)
        self.assertEqual(len(data),10)
        self.assertAlmostEqual(data[0],56.0)
        self.assertAlmostEqual(data[1],-78.6)
        for v in data[2:]:
            self.assertNotEqual(v,v)
        
    def testSetGet(self):
        session = self.session()
        a = session.add(self.structure())
        a.push_back(56).push_back(-104.5)
        self.assertEqual(a.size(),2)
        self.assertAlmostEqual(a[1],-104.5)