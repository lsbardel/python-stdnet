'''Test the CSV serializer'''
from stdnet import odm

from examples.data import FinanceTest, Fund

from . import base


class TestFinanceCSV(base.SerializerMixin, FinanceTest):
    serializer = 'csv'
        
    def testTwoModels(self):
        s = yield self.dump()
        self.assertEqual(len(s.data), 1)
        funds = yield Fund.objects.query().all()
        self.assertRaises(ValueError, s.dump, funds)
        self.assertEqual(len(s.data), 1)
        
    def testLoadError(self):
        s = yield self.dump()
        self.assertRaises(ValueError, s.load, 'bla')
        
        
class TestLoadFinanceCSV(base.LoadSerializerMixin, FinanceTest):
    serializer = 'csv'
