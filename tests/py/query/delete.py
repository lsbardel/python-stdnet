'''Delete objects and queries'''
import datetime
from random import randint

from stdnet import odm
from stdnet.utils import test, populate, zip
from stdnet.exceptions import QuerySetError

from examples.models import Instrument, Fund, Position, Dictionary, SimpleModel
from examples.data import FinanceTest

DICT_LEN    = 200
dict_keys   = populate('string', DICT_LEN, min_len=5, max_len=20)
dict_values = populate('string', DICT_LEN, min_len=20, max_len=300)


class TestDeleteSimpleModel(test.CleanTestCase):
    model = SimpleModel
    
    def test_session_delete(self):
        session = self.session()
        query = session.query(self.model)
        with session.begin() as t:
            m = t.add(self.model(code='ciao'))
        yield t.on_result
        ids = yield query.get_field('id').all()
        self.assertEqual(len(ids),1)
        id = ids[0]
        elem = yield query.get(id=id)
        with session.begin() as t:
            t.delete(elem)
        yield t.on_result
        all = yield query.all()
        self.assertEqual(all, [])
        
    def testSimpleQuery(self):
        session = self.session()
        with session.begin() as t:
            t.add(self.model(code='ciao'))
        yield t.on_result
        all = yield session.query(self.model).all()
        self.assertEqual(len(all), 1)
        yield session.query(self.model).delete()
        all = yield session.query(self.model).all()
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
        yield self.async.assertEqual(query.count(), 1)
        qs = query.filter(group='star')
        yield self.async.assertEqual(qs.count(), 0)
        qs = query.filter(group='planet')
        yield self.async.assertEqual(qs.count(),1)
    
    
class update_model(object):
    
    def __init__(self, test):
        self.test = test
        self.session = None
        
    def __call__(self, sender, instances = None, session = None,
                 transaction = None, **kwargs):
        self.session = session
        self.instances = instances
        self.transaction = transaction
        
        
class TestPostDeleteSignal(test.CleanTestCase):
    model = SimpleModel
            
    def setUp(self):
        self.update_model = update_model(self)
        odm.post_delete.connect(self.update_model, sender = self.model)
        
    def tearDown(self):
        odm.post_delete.disconnect(self.update_model, sender = self.model)
        
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
        

class TestDeleteMethod(FinanceTest):
    '''Test the delete method in models and in queries.'''
    def setUp(self):
        self.register()
        
    def test_delete_all(self):
        session = yield self.data.create(self)
        instruments = Instrument.objects.query()
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
        

class TestDeleteScalarFields(FinanceTest):
        
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
        backend = session.backend
        if backend.name == 'redis':
            keys = yield session.keys(Instrument)
            self.assertEqual(len(keys), 1)
            self.assertEqual(keys[0].decode('utf-8'),
                             backend.basekey(Instrument._meta, 'ids'))
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
        

class TestDeleteStructuredFields(test.CleanTestCase):
    model = Dictionary
    
    def setUp(self):
        session = self.session()
        with session.begin() as t:
            t.add(Dictionary(name='test'))
            t.add(Dictionary(name='test2'))
        yield t.on_result
        yield self.async.assertEqual(session.query(Dictionary).count(), 2)
        self.data = dict(zip(dict_keys, dict_values))
    
    def fill(self, name):
        session = self.session()
        d = yield session.query(Dictionary).get(name=name)
        self.assertEqual(len(session._models), 1)
        data = d.data
        self.assertEqual(len(session._models), 1)
        self.assertEqual(data.instance, d)
        self.assertTrue(data.id)
        yield d.data.update(self.data)
        self.async.assertEqual(data.size(), len(self.data))
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
        backend = session.backend
        if backend.name == 'redis':
            keys = yield session.keys(Dictionary)
            self.assertEqual(keys, [])
