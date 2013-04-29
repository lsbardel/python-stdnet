'''Test query meta and corner cases'''
from stdnet import QuerySetError
from stdnet.utils import test

from examples.models import Instrument
from examples.data import FinanceTest


class TestMeta(FinanceTest):
        
    def testEmpty(self):
        empty = self.session().empty(Instrument)
        self.assertEqual(empty.meta, Instrument._meta)
        self.assertEqual(len(empty), 0)
        self.assertEqual(empty.count(), 0)
        self.assertEqual(list(empty), [])
        self.assertEqual(empty.executed, True)
        self.assertEqual(empty.construct(), empty)
        self.assertEqual(empty.items(), [])
        self.assertEqual(empty.all(), [])
        self.assertEqual(empty.intersect(self.query()), empty)
        all = self.query()
        all2 = empty.union(all)
        all = yield all.all()
        all2 = yield all2.all()
        self.assertEqual(set(all), set(all2))
        q = self.query().filter(ccy__in=())
        yield self.async.assertEqual(q.count(), 0)
                
    def testProperties(self):
        query = self.query()
        self.assertFalse(query.executed)
        
    def test_getfield(self):
        query = self.query()
        self.assertRaises(QuerySetError, query.get_field, 'waaaaaaa')
        query = query.get_field('id')
        query2 = query.get_field('id')
        self.assertEqual(query, query2)
        
    def testFilterError(self):
        query = self.query().filter(whoaaaaa='foo')
        self.assertRaises(QuerySetError, query.all)
        
    def testEmptyParameters(self):
        query = self.query().filter(ccy='USD')
        self.assertEqual(query, query.filter())
        self.assertEqual(query, query.exclude())
        
        
class TestMetaRepr(FinanceTest):
    
    @classmethod
    def after_setup(cls):
        cls.data = cls.data_cls(size=cls.size)
        yield cls.data.create(cls)
        
    def testRepr(self):
        query = self.query().filter(ccy='EUR')\
                            .exclude(type=('equity', 'bond'))
        self.assertTrue(str(query))
        # The query is still lazy
        self.assertFalse(query.executed)
        v = yield query.all()
        self.assertTrue(v)
        self.assertEqual(str(query), str(v))
