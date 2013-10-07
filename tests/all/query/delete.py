'''Delete objects and queries'''
import datetime
from random import randint

from stdnet import odm
from stdnet.utils import test, zip

from examples.models import Instrument, Fund, Position, Dictionary, SimpleModel
from examples.data import finance_data, FinanceTest


class DictData(test.DataGenerator):

    def generate(self):
        self.keys   = self.populate(min_len=5, max_len=20)
        self.values = self.populate(min_len=20, max_len=300)
        self.data = dict(zip(self.keys, self.values))


class TestDeleteSimpleModel(test.TestCase):
    model = SimpleModel
    data_cls = DictData

    def test_session_delete(self):
        session = self.session()
        query = session.query(self.model)
        with session.begin() as t:
            m = t.add(self.model(code='ciao'))
        yield t.on_result
        elem = yield query.get(id=m.id)
        with session.begin() as t:
            t.delete(elem)
        yield t.on_result
        all = yield query.filter(id=m.id).all()
        self.assertEqual(all, [])

    def testSimpleQuery(self):
        session = self.session()
        with session.begin() as t:
            t.add(self.model(code='hello'))
            t.add(self.model(code='hello2'))
        yield t.on_result
        query = session.query(self.model).filter(code=('hello','hello2'))
        yield self.async.assertEqual(query.count(), 2)
        yield query.delete()
        query = session.query(self.model).filter(code=('hello','hello2'))
        all = yield query.all()
        self.assertEqual(all, [])

    def test_simple_filter(self):
        session = self.session()
        query = session.query(self.model)
        with session.begin() as t:
            t.add(self.model(code='sun', group='star'))
            t.add(self.model(code='vega', group='star'))
            t.add(self.model(code='sirus', group='star'))
            t.add(self.model(code='pluto', group='planet'))
        yield t.on_result
        with session.begin() as t:
            t.delete(query.filter(group='star'))
        yield t.on_result
        yield self.async.assertEqual(query.filter(group='star').count(), 0)
        rest = query.exclude(group='star').count()
        self.assertTrue(rest)
        qs = query.filter(group='planet')
        yield self.async.assertEqual(qs.count(), 1)


class update_model(object):

    def __init__(self, test):
        self.test = test
        self.session = None

    def __call__(self, signal, sender, instances=None, session=None,
                 transaction=None, **kwargs):
        self.session = session
        self.instances = instances
        self.transaction = transaction


class TestPostDeleteSignal(test.TestWrite):
    model = SimpleModel

    def setUp(self):
        models = self.mapper
        self.update_model = update_model(self)
        models.post_delete.bind(self.update_model, sender=self.model)

    def tearDown(self):
        models = self.mapper
        models.post_delete.unbind(self.update_model, sender=self.model)

    def testSignal(self):
        session = self.session()
        with session.begin() as t:
            m = t.add(self.model(code='ciao'))
            m = t.add(self.model(code='bla'))
        yield t.on_result
        deleted = yield session.query(self.model).delete()
        u = self.update_model
        self.assertEqual(u.session, session)
        self.assertEqual(len(u.instances), 2)


class TestDeleteMethod(test.TestWrite):
    '''Test the delete method in models and in queries.'''
    data_cls = finance_data
    models = (Instrument, Fund, Position)

    def test_delete_all(self):
        session = yield self.data.create(self)
        instruments = self.query()
        count = yield instruments.count()
        self.assertTrue(count)
        ids = yield instruments.delete()
        self.assertTrue(ids)
        self.assertEqual(len(ids), count)

    def testDeleteMultiQueries(self):
        session = yield self.data.create(self)
        query = session.query(Instrument)
        with session.begin() as t:
            t.delete(query.filter(ccy='EUR'))
            t.delete(query.filter(type=('future','bond')))
        yield t.on_result
        all = yield query.all()
        for inst in all:
            self.assertFalse(inst.type in ('future','bond'))
            self.assertNotEqual(inst.ccy,'EUR')


class TestDeleteScalarFields(test.TestWrite):
    data_cls = finance_data
    models = (Instrument, Fund, Position)

    def test_flush_simple_model(self):
        '''Use the class method flush to remove all instances of a
 Model including filters.'''
        session = yield self.data.create(self)
        deleted = yield session.query(Instrument).delete()
        yield self.async.assertEqual(session.query(Instrument).all(), [])
        yield self.async.assertEqual(session.query(Position).all(), [])
        keys = yield session.keys(Instrument)
        if self.backend == 'redis':
            self.assertTrue(len(keys) > 0)

    def testFlushRelatedModel(self):
        session = yield self.data.makePositions(self)
        self.assertTrue(self.data.num_pos > 0)
        yield session.query(Instrument).delete()
        yield self.async.assertEqual(session.query(Instrument).all(), [])
        yield self.async.assertEqual(session.query(Position).all(), [])
        keys = yield session.keys(Instrument)
        if self.backend == 'redis':
            self.assertTrue(len(keys) > 0)

    def testDeleteSimple(self):
        '''Test delete on models without related models'''
        session = yield self.data.create(self)
        t = yield session.query(Instrument).delete()
        all = yield session.query(Instrument).all()
        self.assertEqual(all, [])
        # There should be only keys for indexes and auto id
        backend = session.model(Instrument).backend
        if backend.name == 'redis':
            keys = yield session.keys(Instrument)
            self.assertEqual(len(keys), 1)
            self.assertEqual(keys[0], backend.basekey(Instrument._meta, 'ids'))
            yield session.flush(Instrument)
            keys = yield session.keys(Instrument)
            self.assertEqual(len(keys), 0)

    def testDeleteRelatedOneByOne(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        session = yield self.data.makePositions(self)
        instruments = yield session.query(Instrument).all()
        with session.begin() as t:
            for inst in instruments:
                t.delete(inst)
        yield t.on_result
        yield self.async.assertEqual(session.query(Instrument).all(), [])
        yield self.async.assertEqual(session.query(Position).all(), [])

    def testDeleteRelated(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        session = yield self.data.makePositions(self)
        yield session.query(Instrument).delete()
        yield self.async.assertEqual(session.query(Instrument).all(), [])
        yield self.async.assertEqual(session.query(Position).all(), [])

    def __testDeleteRelatedCounting(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        NP = 20
        N = Instrument.objects.all().count() + NP
        self.makePositions(NP)
        Instrument.objects.all().delete()
        self.assertEqual(Instrument.objects.all().count(),0)
        self.assertEqual(Position.objects.all().count(),0)


class TestDeleteStructuredFields(test.TestWrite):
    model = Dictionary
    data_cls = DictData

    def setUp(self):
        session = self.session()
        with session.begin() as t:
            t.add(Dictionary(name='test'))
            t.add(Dictionary(name='test2'))
        yield t.on_result
        yield self.async.assertEqual(session.query(Dictionary).count(), 2)

    def fill(self, name):
        session = self.session()
        d = yield session.query(Dictionary).get(name=name)
        self.assertEqual(len(session._models), 1)
        data = d.data
        self.assertEqual(len(session._models), 1)
        self.assertTrue(data.field)
        self.assertFalse(data.id)
        yield d.data.update(self.data.data)
        self.async.assertEqual(data.size(), len(self.data.data))
        yield d

    def testSimpleFlush(self):
        session = self.session()
        yield session.flush(Dictionary)
        yield self.async.assertEqual(session.query(Dictionary).count(), 0)
        # Now we check the database if it is empty as it should
        keys = yield session.keys(Dictionary)
        self.assertEqual(len(keys), 0)

    def test_flush_with_data(self):
        yield self.fill('test')
        yield self.fill('test2')
        session = self.session()
        yield session.flush(Dictionary)
        yield self.async.assertEqual(session.query(Dictionary).count(), 0)
        # Now we check the database if it is empty as it should
        backend = self.mapper.dictionary.backend
        if backend.name == 'redis':
            keys = yield session.keys(Dictionary)
            self.assertEqual(keys, [])
