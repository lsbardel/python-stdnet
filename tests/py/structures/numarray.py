import os
from datetime import date

from stdnet import odm, InvalidTransaction
from stdnet.utils import test, encoders, zip
from stdnet.utils.populate import populate

from .base import StructMixin
    
class TestNumberArray(StructMixin, test.TestCase):
    structure = odm.NumberArray
    name = 'numberarray'
    
    def create_one(self):
        a = self.structure()
        return a.push_back(56).push_back(-78.6)
    
    def testSizeResize(self):
        a = yield self.not_empty()
        yield self.async.assertEqual(a.size(), 2)
        yield self.async.assertEqual(len(a), 2)
        yield self.async.assertEqual(a.resize(10), 10)
        data = yield a.items()
        self.assertEqual(len(data), 10)
        self.assertAlmostEqual(data[0], 56.0)
        self.assertAlmostEqual(data[1], -78.6)
        for v in data[2:]:
            self.assertNotEqual(v,v)
        
    def testSetGet(self):
        a = yield self.not_empty()
        yield self.async.assertEqual(a.size(), 2)
        value = yield a[1]
        self.assertAlmostEqual(value, -78.6)