'''AutoId, CompositeId and custom Id tests.'''
from uuid import uuid4
from random import randint

import pulsar

import stdnet
from stdnet import FieldError
from stdnet.utils import test

from examples.models import Task, WordBook, SimpleModel, Instrument


def genid():
    return str(uuid4())[:8]


class Id(test.TestCase):
    '''Test primary key when it is not an AutoIdField.
Use the manager for convenience.'''
    model = Task
    
    @classmethod
    def after_setup(cls):
        cls.register()
        
    def make(self, name='pluto'):
        return Task(id=genid(), name=name).save()
    
    def testCreate(self):
        t1 = yield self.make()
        yield pulsar.async_sleep(0.5)
        t2 = yield self.make()
        self.assertNotEqual(t1.id, t2.id)
        self.assertTrue(t1.timestamp < t2.timestamp)
        
    def testFailSave(self):
        t1 = yield self.make()
        id1 = t1.id
        self.assertEqual(id1, t1._dbdata['id'])
        self.assertTrue(t1.state().persistent)
        t1.id = genid()
        yield t1.save()
        id2 = t1.id
        self.assertEqual(id2, t1._dbdata['id'])
        yield self.async.assertEqual(
                    self.model.objects.query().filter(id=(id1,id2)).count(), 2)
        
    def test_clone(self):
        t1 = yield self.make()
        yield pulsar.async_sleep(0.5)
        t2 = yield t1.clone(id=genid()).save()
        self.assertNotEqual(t1.id, t2.id)
        self.assertEqual(t1.name, t2.name)
        self.assertNotEqual(t1.timestamp, t2.timestamp)
        self.assertTrue(t1.timestamp < t2.timestamp)
        tasks = yield Task.objects.query().filter(id=(t1.id, t2.id)).all()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].id, t2.id)
        self.assertEqual(tasks[1].id, t1.id)
        self.assertTrue(tasks[0].timestamp > tasks[1].timestamp)
        
    def test_delete_and_clone(self):
        t1 = yield self.make()
        yield t1.delete()
        t2 = yield t1.clone(id=genid()).save()
        self.assertNotEqual(t1.id, t2.id)
        self.assertEqual(t1.name, t2.name)
        tasks = yield Task.objects.query().filter(id=(t1.id,t2.id)).all()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].id, t2.id)
        
    def testFail(self):
        t = Task(name='pluto')
        yield self.async.assertRaises(Exception, t.save)


class TestAutoId(test.TestCase):
    models = (SimpleModel, Instrument)
    
    @classmethod
    def after_setup(cls):
        cls.register()
        
    def random_id(self, id=None):
        if self.backend.name == 'mongo':
            from bson.objectid import ObjectId
            return ObjectId()
        else:
            if id:
                return id+1
            else:
                return randint(1,1000)
    
    def testMeta(self):
        pk = self.model._meta.pk
        self.assertEqual(pk.name, 'id')
        self.assertEqual(pk.type, 'auto')
        self.assertEqual(pk.internal_type, None)
        self.assertEqual(pk.python_type, None)
        self.assertEqual(str(pk), 'examples.simplemodel.id')
        self.assertRaises(FieldError, pk.register_with_model,
                          'bla', SimpleModel)
        
    def testCreateWithValue(self):
        # create an instance with an id
        id = self.random_id()
        m1 = yield SimpleModel(id=id, code='bla').save()
        self.assertEqual(m1.id, id)
        self.assertEqual(m1.code, 'bla')
        m2 = yield SimpleModel(code='foo').save()
        id2 = self.random_id(id)
        self.assertEqualId(m2, id2)
        self.assertEqual(m2.code, 'foo')
        qs = yield SimpleModel.objects.query().all()
        self.assertEqual(len(qs), 2)
        self.assertEqual(set(qs), set((m1, m2)))
    
    def testCreateWithValue2(self):
        id = self.random_id()
        m1 = yield Instrument(name='test1', type='bla', ccy='eur').save()
        m2 = yield Instrument(id=id, name='test2', type='foo', ccy='eur').save()
        self.assertEqualId(m1, 1)
        self.assertEqual(m2.id, id)
        qs = yield Instrument.objects.query().all()
        self.assertEqual(len(qs), 2)
        self.assertEqual(set(qs), set((m1,m2)))
    
    
class CompositeId(test.TestCase):
    multipledb = 'redis'
    model = WordBook
    
    @classmethod
    def after_setup(cls):
        cls.register()
        
    def testMeta(self):
        id = self.model._meta.pk
        self.assertEqual(id.type,'composite')
        self.assertEqual(id.fields,('word','book'))
    
    def testCreate(self):
        m = yield self.model(word='hello',book='world').save()
        self.assertEqual(m.id, 'word:hello,book:world')
        all = yield self.model.objects.query().all()
        self.assertEqual(len(all),1)
        m = all[0]
        self.assertEqual(m.word,'hello')
        self.assertEqual(m.book,'world')
        #
        m.word = 'beautiful'
        yield m.save()
        self.assertEqual(m.id,'word:beautiful,book:world')
        yield self.async.assertEqual(self.model.objects.query().count(), 1)
        all = yield self.model.objects.query().all()
        self.assertEqual(len(all), 1)
        m = all[0]
        self.assertEqual(m.word,'beautiful')
        self.assertEqual(m.book,'world')
        