'''Test the JSON serializer'''
from stdnet import test, orm

from examples.data import FinanceTest, Instrument, Fund, Position

from .base import SerializerMixin


class TestFinanceJSON(FinanceTest, SerializerMixin):
    serializer = 'json'
    
    def setUp(self):
        self.register()
        
    def testTwoModels(self):
        s = self.testDump()
        self.assertEqual(len(s.data),1)
        d = s.data[0]
        self.assertEqual(d['model'],str(self.model._meta))
        s.serialize(Fund.objects.query().sort_by('id'))
        self.assertEqual(len(s.data),2)
    
    
        