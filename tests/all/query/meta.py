"""Test query meta and corner cases"""
from examples.data import FinanceTest
from examples.models import Instrument

from stdnet import QuerySetError, odm
from stdnet.utils import test


class TestMeta(FinanceTest):
    def test_session_meta(self):
        models = self.mapper
        session = models.session()
        self.assertEqual(session.router, models)
        self.assertEqual(session.transaction, None)

    def testQueryMeta(self):
        models = self.mapper
        qs = models.instrument.query()
        self.assertIsInstance(qs, odm.Query)
        self.assertEqual(qs.model, models.instrument.model)

    def test_empty_query(self):
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
        self.assertRaises(QuerySetError, query.get_field, "waaaaaaa")
        query = query.get_field("id")
        query2 = query.get_field("id")
        self.assertEqual(query, query2)

    def testFilterError(self):
        query = self.query().filter(whoaaaaa="foo")
        self.assertRaises(QuerySetError, query.all)

    def testEmptyParameters(self):
        query = self.query().filter(ccy="USD")
        self.assertEqual(query, query.filter())
        self.assertEqual(query, query.exclude())


class TestMetaWithData(FinanceTest):
    @classmethod
    def after_setup(cls):
        return cls.data.create(cls)

    def test_repr(self):
        models = self.mapper
        # make sure there is at least one of them
        yield models.instrument.new(name="a123345566", ccy="EUR", type="future")
        query = self.query().filter(ccy="EUR").exclude(type=("equity", "bond"))
        self.assertTrue(str(query))
        # The query is still lazy
        self.assertFalse(query.executed)
        v = yield query.all()
        self.assertTrue(v)
        self.assertEqual(str(query), str(v))
