'''Delete objects and queries'''
import datetime
from random import randint

from stdnet import test, orm
from stdnet.utils import populate, zip
from stdnet.exceptions import QuerySetError

from examples.models import Instrument, Fund, Position, Dictionary, SimpleModel
from examples.data import FinanceTest

DICT_LEN    = 200
dict_keys   = populate('string', DICT_LEN, min_len = 5, max_len = 20)
dict_values = populate('string', DICT_LEN, min_len = 20, max_len = 300)


class TestDeleteSimpleModel(test.TestCase):
    model = SimpleModel
    
    def testSessionDelete(self):
        session = self.session()
        query = session.query(self.model)
        with session.begin():
            m = session.add(self.model(code = 'ciao'))
            
        ids = query.get_field('id').all()
        self.assertEqual(len(ids),1)
        id = ids[0]
        with session.begin():
            session.delete(query.get(id = id))
        self.assertEqual(query.all(),[])
        
    def testSimpleQuery(self):
        session = self.session()
        with session.begin():
            session.add(self.model(code = 'ciao'))
        all = session.query(self.model).all()
        self.assertEqual(len(all),1)
        session.query(self.model).delete()
        all = session.query(self.model).all()
        self.assertEqual(all,[])
        
    def testSimpleFilter(self):
        session = self.session()
        query = session.query(self.model)
        with session.begin():
            session.add(self.model(code = 'sun', group = 'star'))
            session.add(self.model(code = 'vega', group = 'star'))
            session.add(self.model(code = 'sirus', group = 'star'))
            session.add(self.model(code = 'pluto', group = 'planet'))
        with session.begin() as t:
            session.delete(query.filter(group = 'star'))
        self.assertTrue(t.done)
        self.assertEqual(len(query.all()),1)
        qs = query.filter(group = 'star')
        self.assertEqual(qs.count(),0)
        qs = query.filter(group = 'planet')
        self.assertEqual(qs.count(),1)
     
        
class update_model(object):
    
    def __init__(self, test):
        self.test = test
        self.session = None
        
    def __call__(self, sender, instances = None, session = None,
                 transaction = None, **kwargs):
        self.session = session
        self.instances = instances
        self.transaction = transaction
        
        
class TestPostDeleteSignal(test.TestCase):
    model = SimpleModel
            
    def setUp(self):
        self.update_model = update_model(self)
        orm.post_delete.connect(self.update_model, sender = self.model)
        
    def tearDown(self):
        orm.post_delete.disconnect(self.update_model, sender = self.model)
        
    def testSignal(self):
        session = self.session()
        
        with session.begin():
            m = session.add(self.model(code = 'ciao'))
            m = session.add(self.model(code = 'bla'))
            
        session.query(self.model).delete()
        u = self.update_model
        self.assertEqual(u.session,session)
        self.assertEqual(sorted(u.instances),[1,2])
        

class TestDeleteScalarFields(FinanceTest):
        
    def testFlushSimpleModel(self):
        '''Use the class method flush to remove all instances of a
 Model including filters.'''
        session = self.data.create(self)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertEqual(session.query(Position).all(),[])
        keys = list(session.keys(Instrument))
        self.assertTrue(len(keys)>0)
        
    def testFlushRelatedModel(self):
        session = self.data.makePositions(self)
        self.assertTrue(session.query(Position).count()>0)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertEqual(session.query(Position).all(),[])
        keys = list(session.keys(Instrument))
        self.assertTrue(len(keys)>0)
        
    def testDeleteSimple(self):
        '''Test delete on models without related models'''
        session = self.data.create(self)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        # There should be only keys for indexes and auto id
        backend = session.backend
        if backend.name == 'redis':
            keys = list(session.keys(Instrument))
            self.assertEqual(len(keys),1)
            self.assertEqual(keys[0],backend.basekey(Instrument._meta,'ids'))
            session.flush(Instrument)
            keys = list(session.keys(Instrument))
            self.assertEqual(len(keys),0)

    def testDeleteRelatedOneByOne(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        self.data.makePositions(self)
        session = self.session()
        with session.begin():
            for inst in session.query(Instrument):
                session.delete(inst)
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertEqual(session.query(Position).all(),[])
                
    def testDeleteRelated(self):
        '''Test delete on models with related models. This is a crucial
test as it involves lots of operations and consistency checks.'''
        # Create Positions which hold foreign keys to Instruments
        session = self.data.makePositions(self)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertEqual(session.query(Position).all(),[])
        
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
        

class TestDeleteStructuredFields(test.TestCase):
    model = Dictionary
    
    def setUp(self):
        '''Create Instruments and Funds commiting at the end for speed'''
        session = self.session()
        with session.begin():
            session.add(Dictionary(name = 'test'))
            session.add(Dictionary(name = 'test2'))
        self.assertEqual(session.query(Dictionary).count(),2)
        self.data = dict(zip(dict_keys,dict_values))
    
    def fill(self, name):
        session = self.session()
        d = session.query(Dictionary).get(name = name)
        self.assertEqual(len(session._models),1)
        data = d.data
        self.assertEqual(len(session._models),2)
        self.assertEqual(data.instance,d)
        self.assertTrue(data.id)
        d.data.update(self.data)
        self.assertEqual(d.data.size(),0)
        session.commit()
        self.assertEqual(data.size(),len(self.data))
        return d
    
    def testSimpleFlush(self):
        session = self.session()
        session.flush(Dictionary)
        self.assertEqual(session.query(Dictionary).count(),0)
        # Now we check the database if it is empty as it should
        keys = list(session.keys(Dictionary))
        self.assertEqual(len(keys),0)
        
    def testFlushWithData(self):
        self.fill('test')
        self.fill('test2')
        session = self.session()
        session.flush(Dictionary)
        self.assertEqual(session.query(Dictionary).count(),0)
        # Now we check the database if it is empty as it should
        backend = session.backend
        if backend.name == 'redis':
            keys = list(session.keys(Dictionary))
            self.assertEqual(keys,[])
    
