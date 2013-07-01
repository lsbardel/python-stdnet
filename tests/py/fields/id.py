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
    
    def make(self, name='pluto'):
        return self.mapper.task.new(id=genid(), name=name)
    
    def test_create(self):
        t1 = yield self.make()
        yield pulsar.async_sleep(0.5)
        t2 = yield self.make()
        self.assertNotEqual(t1.id, t2.id)
        self.assertTrue(t1.timestamp < t2.timestamp)
        
    def test_change_id(self):
        session = self.session()
        t1 = yield self.make()
        id1 = t1.id
        self.assertEqual(id1, t1._dbdata['id'])
        self.assertTrue(t1.get_state().persistent)
        id2 = genid()
        t1.id = id2
        self.assertEqual(id1, t1._dbdata['id'])
        self.assertNotEqual(id2, t1._dbdata['id'])
        yield session.add(t1)
        self.assertEqual(id2, t1.id)
        self.assertEqual(id2, t1._dbdata['id'])
        yield self.async.assertEqual(self.query().filter(id=(id1, id2)).count(), 1)
        
    def test_clone(self):
        t1 = yield self.make()
        session = t1.session
        yield pulsar.async_sleep(0.5)
        t2 = yield session.add(t1.clone(id=genid()))
        self.assertNotEqual(t1.id, t2.id)
        self.assertEqual(t1.name, t2.name)
        self.assertNotEqual(t1.timestamp, t2.timestamp)
        self.assertTrue(t1.timestamp < t2.timestamp)
        tasks = yield self.query().filter(id=(t1.id, t2.id)).all()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].id, t2.id)
        self.assertEqual(tasks[1].id, t1.id)
        self.assertTrue(tasks[0].timestamp > tasks[1].timestamp)
        
    def test_delete_and_clone(self):
        t1 = yield self.make()
        session = t1.session
        res = yield session.delete(t1)
        tasks = yield self.query().filter(id=t1.id).all()
        self.assertEqual(len(tasks), 0)
        t2 = yield session.add(t1.clone(id=genid()))
        self.assertNotEqual(t1.id, t2.id)
        self.assertEqual(t1.name, t2.name)
        tasks = yield self.query().filter(id=(t1.id, t2.id)).all()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].id, t2.id)
        
    def test_fail(self):
        session = self.session()
        t = Task(name='pluto')
        yield self.async.assertRaises(Exception, session.add, t)


class TestAutoId(test.TestCase):
    models = (SimpleModel, Instrument)
        
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
        models = self.mapper
        id = self.random_id()
        m1 = yield models.simplemodel.new(id=id, code='bla')
        self.assertEqual(m1.id, id)
        self.assertEqual(m1.code, 'bla')
        m2 = yield models.simplemodel.new(code='foo')
        id2 = self.random_id(id)
        self.assertEqualId(m2, id2)
        self.assertEqual(m2.code, 'foo')
        qs = yield models.simplemodel.query().all()
        self.assertEqual(len(qs), 2)
        self.assertEqual(set(qs), set((m1, m2)))
    
    def testCreateWithValue2(self):
        models = self.mapper
        id = self.random_id()
        m1 = yield models[Instrument].new(name='test1', type='bla', ccy='eur')
        m2 = yield models.instrument.new(id=id, name='test2', type='foo', ccy='eur')
        self.assertEqualId(m1, 1)
        self.assertEqual(m2.id, id)
        qs = yield models.instrument.query().all()
        self.assertEqual(len(qs), 2)
        self.assertEqual(set(qs), set((m1,m2)))
    
    
class CompositeId(test.TestCase):
    model = WordBook
    
    def create(self, word, book):
        session = self.session()
        m = yield session.add(self.model(word=word, book=book))
        self.assertEqual(m.pkvalue(), m.id)
        id = m.id
        m = yield session.query(self.model).get(word=word, book=book)
        self.assertEqual(m.word, word)
        self.assertEqual(m.book, book)
        self.assertEqual(m.id, id)
        yield m
        
    def testMeta(self):
        id = self.model._meta.pk
        self.assertEqual(id.type, 'composite')
        fields = id.fields
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0], self.model._meta.dfields['word'])
        self.assertEqual(fields[1], self.model._meta.dfields['book'])
    
    def test_value(self):
        m = self.model(book='world', word='hello')
        self.assertFalse(m.id)
        value = m.pkvalue()
        self.assertTrue(value)
        self.assertEqual(value, hash(('hello', 'world')))
        m = self.model(book='hello', word='world')
        self.assertNotEqual(value, m.pkvalue())
        
    def test_create(self):
        return self.create('hello', 'world')
        
    def test_change(self):
        m = yield self.create('ciao', 'libro')
        session = m.session
        id = m.id
        m.word = 'beautiful'
        self.assertNotEqual(m.pkvalue(), id)
        yield session.add(m)
        self.assertNotEqual(m.id, id)
        self.assertEqual(m.word, 'beautiful') 
        query = self.query()
        yield self.async.assertEqual(query.filter(id=id).count(), 0)
        yield self.async.assertEqual(query.filter(id=m.id).count(), 1)
        yield self.async.assertEqual(query.filter(word='ciao', book='libro').count(), 0)
        m2 = yield query.get(word='beautiful', book='libro')
        self.assertEqual(m, m2)
        