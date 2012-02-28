'''Test the CSV serializer'''
from stdnet import test, orm

from examples.data import FinanceTest, Instrument, Fund, Position

from .base import SerializerMixin


class TestFinanceJSON(FinanceTest, SerializerMixin):
    serializer = 'csv'
    
    def setUp(self):
        self.register()
        
    def testTwoModels(self):
        s = self.testDump()
        self.assertEqual(len(s.data),1)
        self.assertRaises(ValueError, s.serialize,
                          Fund.objects.query().sort_by('id'))
        self.assertEqual(len(s.data),1)