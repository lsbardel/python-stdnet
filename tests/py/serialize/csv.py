'''Test the CSV serializer'''
from stdnet import odm
from stdnet.utils import test

from examples.data import FinanceTest, Instrument, Fund, Position

from .base import SerializerMixin


class TestFinanceCSV(FinanceTest, SerializerMixin):
    serializer = 'csv'
    
    def setUp(self):
        self.register()
        
    def testTwoModels(self):
        s = yield self.testDump()
        self.assertEqual(len(s.data), 1)
        funds = yield Fund.objects.query().all()
        self.assertRaises(ValueError, s.dump, funds)
        self.assertEqual(len(s.data), 1)
        
    def testLoadError(self):
        s = yield self.testDump()
        self.assertRaises(ValueError, s.load, 'bla')