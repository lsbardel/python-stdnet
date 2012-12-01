'''Test query meta and corner cases'''
from stdnet import QuerySetError
from stdnet.utils import test

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
        self.assertEqual(empty.items(),[])
        self.assertEqual(empty.all(),[])
        self.assertEqual(empty.intersect(Instrument.objects.query()),empty)
        all = Instrument.objects.query()
        all2 = empty.union(all)
        self.assertEqual(set(all),set(all2))
        q = Instrument.objects.filter(ccy__in = ())
        self.assertEqual(q.count(),0)
                
    def testProperties(self):
        query = Instrument.objects.query()
        self.assertFalse(query.executed)
        
    def test_getfield(self):
        query = Instrument.objects.query()
        self.assertRaises(QuerySetError, query.get_field, 'waaaaaaa')
        query = query.get_field('id')
        query2 = query.get_field('id')
        self.assertEqual(query,query2)
        
    def testFilterError(self):
        query = Instrument.objects.filter(whoaaaaa='foo')
        self.assertRaises(QuerySetError, query.all)
        
    def testRepr(self):
        self.data.create(self)
        query = Instrument.objects.filter(ccy='EUR')\
                                  .exclude(type=('equity','bond'))
        self.assertTrue(str(query))
        # The query is still lazy
        self.assertFalse(query.cache())
        v = query.all()
        self.assertTrue(v)
        self.assertEqual(str(query), str(v))
        
    def testEmptyParameters(self):
        query = Instrument.objects.filter(ccy='USD')
        self.assertEqual(query, query.filter())
        self.assertEqual(query, query.exclude())