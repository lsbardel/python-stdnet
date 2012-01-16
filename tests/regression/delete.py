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


class TestSimpleModel(test.TestCase):
    model = SimpleModel
    
    def testSimple(self):
        session = self.session()
        with session.begin():
            session.add(self.model(code = 'ciao'))
        all = session.query(self.model).all()
        self.assertEqual(len(all),1)
        session.query(self.model).delete()
        all = session.query(self.model).all()
        self.assertEqual(all,[])


class TestDeleteScalarFields(FinanceTest):
        
    def testFlushSimpleModel(self):
        '''Use the class method flush to remove all instances of a
 Model including filters.'''
        session = self.data.create(self)
        self.assertTrue(session.flush(Instrument)>self.data.num_insts)
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertTrue(session.flush(Fund))
        # Now we check the database if it is empty as it should
        keys = list(session.backend.model_keys(Instrument))
        self.assertEqual(len(keys),0)
        
    def testFlushRelatedModel(self):
        session = self.data.makePositions(self)
        self.assertTrue(session.flush(Instrument)>self.data.num_insts)
        self.assertEqual(session.query(Instrument).all(),[])
        self.assertTrue(session.flush(Fund))
        # Now we check the database if it is empty as it should
        keys = list(session.backend.model_keys(Instrument))
        self.assertEqual(len(keys),0)
        keys = list(session.backend.model_keys(Position))
        self.assertEqual(len(keys),0)
        
    def testDeleteSimple(self):
        '''Test delete on models without related models'''
        session = self.data.create(self)
        session.query(Instrument).delete()
        self.assertEqual(session.query(Instrument).all(),[])
        #There should be only one key in the database,
        # The one used to autoincrement the Instrument ids
        keys = list(Instrument._meta.cursor.keys())
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
        for inst in Instrument.objects.all():
            inst.delete()
        self.assertEqual(Instrument.objects.all().count(),0)
        self.assertEqual(Position.objects.all().count(),0)
                
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
        Dictionary(name = 'test').save()
        Dictionary(name = 'test2').save()
        self.assertEqual(Dictionary.objects.all().count(),2)
        self.data = dict(zip(dict_keys,dict_values))
    
    def fill(self, name):
        d = Dictionary.objects.get(name = name)
        data = d.data
        d.data.update(self.data)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(self.data))
        return Dictionary.objects.get(name = name)
    
    def testSimpleFlush(self):
        Dictionary.flush()
        self.assertEqual(Dictionary.objects.all().count(),0)
        # Now we check the database if it is empty as it should
        keys = list(Dictionary._meta.cursor.keys())
        self.assertEqual(len(keys),0)
        
    def testFlushWithData(self):
        self.fill('test')
        self.fill('test2')
        Dictionary.flush()
        self.assertEqual(Dictionary.objects.all().count(),0)
        # Now we check the database if it is empty as it should
        keys = list(Dictionary._meta.cursor.keys())
        self.assertEqual(len(keys),0)
    
