'''Tests meta classes and corner cases of the library'''
import inspect
from datetime import datetime

from stdnet import odm
from stdnet.utils import test, populate, pickle
from stdnet.exceptions import QuerySetError
from stdnet.odm import model_to_dict, model_iterator
from stdnet.odm.base import StdNetType

from examples.models import SimpleModel, ComplexModel
from examples.data import FinanceTest, Instrument, Fund, Position


class TestInspectionAndComparison(FinanceTest):
    
    def setUp(self):
        self.register()
        
    def testSimple(self):
        d = model_to_dict(Instrument)
        self.assertFalse(d)
        inst = Instrument(name='erz12', type='future', ccy='EUR').save()
        d = model_to_dict(inst)
        self.assertTrue(len(d),3)
        
    def testEqual(self):
        inst = Instrument(name='erz12', type='future', ccy='EUR').save()
        id = inst.id
        b = Instrument.objects.get(id=id)
        self.assertEqual(b.id,id)
        self.assertTrue(inst == b)
        self.assertFalse(inst != b)
        f = Fund(name='bla', ccy='EUR').save()
        self.assertFalse(inst == f)
        self.assertTrue(inst != f)
        
    def testNotEqual(self):
        inst = Instrument(name='erz12', type='future', ccy='EUR').save()
        inst2 = Instrument(name='edz14', type='future', ccy='USD').save()
        id = inst.id
        b = Instrument.objects.get(id = id)
        self.assertEqual(b.id,id)
        self.assertFalse(inst2 == b)
        self.assertTrue(inst2 != b)
        
    def testHash(self):
        '''Test model instance hash'''
        inst = Instrument(name = 'erz12', type = 'future', ccy = 'EUR')
        h0 = hash(inst)
        self.assertTrue(h0)
        inst.save()
        h = hash(inst)
        self.assertTrue(h)
        self.assertNotEqual(h,h0)
        
    def testmodelFromHash(self):
        m = odm.get_model_from_hash(Instrument._meta.hash)
        self.assertEqual(m, Instrument)
        
    def testUniqueId(self):
        '''Test model instance unique id across different model'''
        inst = Instrument(name = 'erz12', type = 'future', ccy = 'EUR')
        self.assertRaises(inst.DoesNotExist, lambda : inst.uuid)
        inst.save()
        v = inst.uuid.split('.') # <<model hash>>.<<instance id>>
        self.assertEqual(len(v),2)
        self.assertEqual(v[0],inst._meta.hash)
        self.assertEqual(v[1],str(inst.id))
        
    def testModelValueError(self):
        self.assertRaises(ValueError, Instrument, bla = 'foo')
        self.assertRaises(ValueError, Instrument, name = 'bee', bla = 'foo')
        self.assertRaises(ValueError, Instrument, name = 'bee', bla = 'foo',
                          foo = 'pippo')


class PickleSupport(test.CleanTestCase):
    model = Instrument
    
    def setUp(self):
        self.register()
        
    def testSimple(self):
        inst = Instrument(name='erz12', type='future', ccy='EUR').save()
        p = pickle.dumps(inst)
        inst2 = pickle.loads(p)
        self.assertEqual(inst,inst2)
        self.assertEqual(inst.name,inst2.name)
        self.assertEqual(inst.type,inst2.type)
        self.assertEqual(inst.ccy,inst2.ccy)
        
    def testTempDictionary(self):
        inst = Instrument(name = 'erz12', type = 'future', ccy = 'EUR').save()
        self.assertTrue('cleaned_data' in inst._dbdata)
        p = pickle.dumps(inst)
        inst2 = pickle.loads(p)
        self.assertFalse('cleaned_data' in inst2._dbdata)
        inst2.save()
        self.assertTrue('cleaned_data' in inst._dbdata)
        

class TestRegistration(test.CleanTestCase):
    
    def testModelIterator(self):
        g = model_iterator('examples')
        self.assertTrue(inspect.isgenerator(g))
        d = list(g)
        self.assertTrue(d)
        for m in d:
            self.assertTrue(inspect.isclass(m))
            self.assertTrue(isinstance(m,StdNetType))


class TestStdModelMethods(test.CleanTestCase):
    model = SimpleModel
    
    def setUp(self):
        self.register()
        
    def testClone(self):
        s = SimpleModel(code='pluto', group='planet',
                        cached_data='blabla').save()
        self.assertEqual(s.cached_data,b'blabla')
        id = self.assertEqualId(s, 1)
        c = s.clone()
        self.assertEqual(c.id, None)
        self.assertFalse(c.cached_data)
        
    def test_clear_cache_fields(self):
        fields = self.model._meta.dfields
        self.assertTrue(fields['timestamp'].as_cache)
        self.assertFalse(fields['timestamp'].required)
        self.assertFalse(fields['timestamp'].index)
        m = self.model(code = 'bla', timestamp = datetime.now()).save()
        self.assertTrue(m.timestamp)
        m.clear_cache_fields()
        self.assertEqual(m.timestamp,None)
        m.save()
        m = self.model.objects.get(id = 1)
        self.assertEqual(m.timestamp,None)
        

class TestComplexModel(test.CleanTestCase):
    model = ComplexModel
    
    def setUp(self):
        self.register()
    
    def testJsonClear(self):
        m = self.model(name ='bla',
                       data = {'italy':'rome', 'england':'london'}).save()
        m = self.model.objects.query().load_only('name').get(id = 1)
        self.assertFalse(m.has_all_data)
        m.data = {'france':'paris'}
        m.save()
        m = self.model.objects.query().get(id = 1)
        self.assertEqual(m.data,{'italy':'rome',
                                 'england':'london',
                                 'france':'paris'})
        self.assertEqual(m.data__italy,'rome')
        m.data = None
        m.save()
        m = self.model.objects.query().get(id = 1)
        self.assertEqual(m.data, {})