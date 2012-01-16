'''Delete objects and queries'''
import datetime
from random import randint

from stdnet import test
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
        query.filter(group = 'star').delete()
        qs = query.filter(group = 'star')
        self.assertEqual(qs.count(),0)
        qs = query.filter(group = 'planet')
        self.assertEqual(qs.count(),1)


class TestDeleteScalarFields(FinanceTest):
        
    def testFlushSimpleModel(self):
        '''Use the class method flush to remove all instances of a
 Model including filters.'''
        session = self.data.create(self)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertEqual(session.query(Position).all(),[])
        keys = list(session.backend.model_keys(Instrument))
        self.assertTrue(len(keys)>0)
        
    def testFlushRelatedModel(self):
        session = self.data.makePositions(self)
        self.assertTrue(session.query(Position).count()>0)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertEqual(session.query(Position).all(),[])
        keys = list(session.backend.model_keys(Instrument))
        self.assertTrue(len(keys)>0)
        
    def testDeleteSimple(self):
        '''Test delete on models without related models'''
        session = self.data.create(self)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        # There should be only keys for indexes and auto id
        keys = list(session.backend.model_keys(Instrument))
        self.assertEqual(len(keys),1)
        self.assertEqual(keys[0],Instrument._meta.autoid())
        Instrument.flush()
        keys = list(Instrument._meta.cursor.keys())
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
        session.query(Position).delete()
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
        data = d.data
        d.data.update(self.data)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(self.data))
        return Dictionary.objects.get(name = name)
    
    def testSimpleFlush(self):
        session = self.session()
        session.flush(Dictionary)
        self.assertEqual(session.query(Dictionary).count(),0)
        # Now we check the database if it is empty as it should
        keys = list(session.backend.model_keys(Dictionary))
        self.assertEqual(len(keys),0)
        
    def testFlushWithData(self):
        self.fill('test')
        self.fill('test2')
        Dictionary.flush()
        self.assertEqual(Dictionary.objects.all().count(),0)
        # Now we check the database if it is empty as it should
        keys = list(Dictionary._meta.cursor.keys())
        self.assertEqual(len(keys),0)
    
