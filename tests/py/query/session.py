'''Sessions and transactions management'''
from pulsar.apps.test import sequential

from stdnet import odm, getdb

from stdnet.utils import test
from stdnet.conf import settings
from stdnet.utils import gen_unique_id

from examples.models import SimpleModel, Instrument

@sequential
class TestSession(test.TestCase):
    model = SimpleModel
    
    def setUp(self):
        self.register()
        
    def tearDown(self):
        self.clear_all()
        
    def testQueryMeta(self):
        session = self.session()
        self.assertEqual(len(session._models), 0)
        qs = session.query(SimpleModel)
        self.assertTrue(isinstance(qs, odm.Query))
    
    def test_simple_create(self):
        session = self.session()
        session.begin()
        self.assertTrue(session.transaction)
        m = SimpleModel(code='pluto', group='planet')
        session.add(m)
        self.assertTrue(m in session)
        sm = session.model(m._meta)
        self.assertEqual(len(sm.new), 1)
        self.assertEqual(len(sm.modified), 0)
        self.assertEqual(len(sm.deleted), 0)
        self.assertTrue(m in sm.new)
        t = yield session.commit()
        self.assertEqualId(m, 1)
        
    def test_create_objects(self):
        # Tests a session with two models. This was for a bug
        session = self.session()
        with session.begin() as t:
            t.add(SimpleModel(code='pluto',group='planet'))
            t.add(Instrument(name='bla',ccy='EUR',type='equity'))
        # The transaction is complete when the on_commit is not asynchronous
        yield t.on_result
        yield self.async.assertEqual(session.query(SimpleModel).count(), 1)
        yield self.async.assertEqual(session.query(Instrument).count(), 1)
        
    def test_simple_filter(self):
        session = self.session()
        with session.begin() as t:
            t.add(SimpleModel(code='pluto', group='planet'))
            t.add(SimpleModel(code='venus', group='planet'))
            t.add(SimpleModel(code='sun', group='star'))
        yield t.on_result
        query = session.query(SimpleModel)
        yield self.async.assertEqual(query.count(), 3)
        self.assertEqual(query.session, session)
        all = yield query.all()
        self.assertEqual(len(all), 3)
        qs = query.filter(group='planet')
        self.assertFalse(qs.executed)
        yield self.async.assertEqual(qs.count(), 2)
        self.assertTrue(qs.executed)
        qs = query.filter(group='star')
        yield self.async.assertEqual(qs.count(), 1)
        qs = query.filter(group='bla')
        yield self.async.assertEqual(qs.count(), 0)
        
    def test_modify_index_field(self):
        session = self.session()
        with session.begin() as t:
            t.add(SimpleModel(code='pluto', group='planet'))
        yield t.on_result
        query = session.query(SimpleModel)
        qs = query.filter(group='planet')
        yield self.async.assertEqual(qs.count(), 1)
        el = yield qs[0]
        id = self.assertEqualId(el, 1)
        session = self.session()
        el.group = 'smallplanet'
        with session.begin() as t:
            t.add(el)
        yield t.on_result    
        yield self.async.assertEqual(session.query(self.model).count(), 1)
        self.assertEqualId(el, id, True)
        # lets get it from the server
        qs = session.query(self.model).filter(id=id)
        yield self.async.assertEqual(qs.count(), 1)
        el = yield qs[0]
        self.assertEqual(el.code, 'pluto')
        self.assertEqual(el.group, 'smallplanet')
        # now filter on group
        qs = session.query(self.model).filter(group='smallplanet')
        yield self.async.assertEqual(qs.count(), 1)
        el = yield qs[0]
        self.assertEqual(el.id, id)
        # now filter on old group
        qs = session.query(self.model).filter(group='planet')
        yield self.async.assertEqual(qs.count(), 0)
    
    