'''Test query meta and corner cases'''
from stdnet import test, QuerySetError

from examples.models import Instrument
from examples.data import FinanceTest


class TestMeta(FinanceTest):
    
    def setUp(self):
        self.register()
        
    def testEmpty(self):
        empty = Instrument.objects.empty()
        self.assertEqual(empty.meta,Instrument._meta)
        self.assertEqual(len(empty),0)
        self.assertEqual(empty.count(),0)
        self.assertEqual(list(empty),[])
        self.assertEqual(empty.executed,True)
        self.assertEqual(empty.construct(),empty)
        
    def testProperties(self):
        query = Instrument.objects.query()
        self.assertFalse(query.executed)
        
    def test_getfield(self):
        query = Instrument.objects.query()
        self.assertRaises(QuerySetError, query.get_field, 'waaaaaaa')
        query = query.get_field('id')
        query2 = query.get_field('id')
        self.assertEqual(query,query2)
        
    