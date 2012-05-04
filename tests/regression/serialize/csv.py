'''Test the CSV serializer'''
from stdnet import test, odm

from examples.data import FinanceTest, Instrument, Fund, Position

from .base import SerializerMixin


class TestFinanceCSV(FinanceTest, SerializerMixin):
    serializer = 'csv'
    
    def setUp(self):
        self.register()
        
    def testTwoModels(self):
        s = self.testDump()
        self.assertEqual(len(s.data),1)
        self.assertRaises(ValueError, s.serialize,
                          Fund.objects.query().sort_by('id'))
        self.assertEqual(len(s.data),1)
        
    def testLoadError(self):
        s = self.testDump()
        self.assertRaises(ValueError, s.load, 'bla')