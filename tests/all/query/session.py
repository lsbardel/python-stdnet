"""Sessions and transactions management"""
from examples.models import Instrument, SimpleModel

from stdnet import getdb, odm
from stdnet.utils import gen_unique_id, test


class TestSession(test.TestWrite):
    models = (SimpleModel, Instrument)

    def test_simple_create(self):
        models = self.mapper
        session = models.session()
        self.assertFalse(session.transaction)
        session.begin()
        self.assertTrue(session.transaction)
        m = models.simplemodel(code="pluto", group="planet")
        self.assertEqual(m, session.add(m))
        self.assertTrue(m in session)
        sm = session.model(m)
        self.assertEqual(len(sm.new), 1)
        self.assertEqual(len(sm.modified), 0)
        self.assertEqual(len(sm.deleted), 0)
        self.assertTrue(m in sm.new)
        t = yield session.commit()
        self.assertTrue(t)
        self.assertEqualId(m, 1)
        self.assertFalse(session.dirty)

    def test_create_objects(self):
        # Tests a session with two models. This was for a bug
        models = self.mapper
        with models.session().begin() as t:
            t.add(models.simplemodel(code="pluto", group="planet"))
            t.add(models.instrument(name="bla", ccy="EUR", type="equity"))
        # The transaction is complete when the on_commit is not asynchronous
        yield t.on_result
        yield self.async.assertEqual(models.simplemodel.query().count(), 1)
        yield self.async.assertEqual(models.instrument.query().count(), 1)

    def test_simple_filter(self):
        models = self.mapper
        session = models.session()
        with session.begin() as t:
            t.add(SimpleModel(code="pluto", group="planet"))
            t.add(SimpleModel(code="venus", group="planet"))
            t.add(SimpleModel(code="sun", group="star"))
        yield t.on_result
        query = session.query(SimpleModel)
        yield self.async.assertEqual(query.count(), 3)
        all = yield query.all()
        self.assertEqual(len(all), 3)
        qs = query.filter(group="planet")
        self.assertFalse(qs.executed)
        yield self.async.assertEqual(qs.count(), 2)
        self.assertTrue(qs.executed)
        qs = query.filter(group="star")
        yield self.async.assertEqual(qs.count(), 1)
        qs = query.filter(group="bla")
        yield self.async.assertEqual(qs.count(), 0)

    def test_modify_index_field(self):
        session = self.session()
        with session.begin() as t:
            t.add(SimpleModel(code="pluto", group="planet"))
        yield t.on_result
        query = session.query(SimpleModel)
        qs = query.filter(group="planet")
        yield self.async.assertEqual(qs.count(), 1)
        el = yield qs[0]
        id = self.assertEqualId(el, 1)
        session = self.session()
        el.group = "smallplanet"
        with session.begin() as t:
            t.add(el)
        yield t.on_result
        yield self.async.assertEqual(session.query(self.model).count(), 1)
        self.assertEqualId(el, id, True)
        # lets get it from the server
        qs = session.query(self.model).filter(id=id)
        yield self.async.assertEqual(qs.count(), 1)
        el = yield qs[0]
        self.assertEqual(el.code, "pluto")
        self.assertEqual(el.group, "smallplanet")
        # now filter on group
        qs = session.query(self.model).filter(group="smallplanet")
        yield self.async.assertEqual(qs.count(), 1)
        el = yield qs[0]
        self.assertEqual(el.id, id)
        # now filter on old group
        qs = session.query(self.model).filter(group="planet")
        yield self.async.assertEqual(qs.count(), 0)
