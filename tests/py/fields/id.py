'''AutoId, CompositeId and custom Id tests.'''
from uuid import uuid4
from random import randint
import time

import stdnet
from stdnet import FieldError
from stdnet.utils import test

from examples.models import Task, WordBook, SimpleModel


def genid():
    return str(uuid4())[:8]


class Id(test.CleanTestCase):
    '''Test primary key when it is not an AutoField.
Use the manager for convenience.'''
    model = Task
    
    def setUp(self):
        self.register()
        
    def make(self):
        return Task(id=genid(), name='pluto').save()
    
    def testCreate(self):
        t1 = self.make()
        time.sleep(0.01)
        t2 = self.make()
        self.assertNotEqual(t1.id,t2.id)
        self.assertTrue(t1.timestamp<t2.timestamp)
        
    def testFailSave(self):
        t1 = self.make()
        self.assertEqual(t1.id, t1._dbdata['id'])
        self.assertTrue(t1.state().persistent)
        t1.id = genid()
        t1.save()
        self.assertEqual(t1.id, t1._dbdata['id'])
        self.assertEqual(self.model.objects.query().count(), 2)
        
    def test_clone(self):
        t1 = self.make()
        time.sleep(0.01)
        t2 = t1.clone(id=genid()).save()
        self.assertNotEqual(t1.id, t2.id)
        self.assertEqual(t1.name, t2.name)
        self.assertNotEqual(t1.timestamp, t2.timestamp)
        self.assertTrue(t1.timestamp < t2.timestamp)
        tasks = Task.objects.query()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].id, t2.id)
        self.assertEqual(tasks[1].id, t1.id)
        self.assertTrue(tasks[0].timestamp > tasks[1].timestamp)
        
    def test_delete_and_clone(self):
        t1 = self.make()
        t1 = Task.objects.get(id = t1.id)
        t1.delete()
        t2 = t1.clone(id = genid()).save()
        self.assertEqual(t1.name,t2.name)
        tasks = list(Task.objects.query())
        self.assertEqual(len(tasks),1)
        self.assertEqual(tasks[0].id,t2.id)
        
    def testFail(self):
        t = Task(name = 'pluto')
        self.assertRaises(Exception, t.save)


class TestAutoId(test.CleanTestCase):
    model = SimpleModel
    
    def setUp(self):
        self.register()
        
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
        self.assertEqual(pk.name,'id')
        self.assertEqual(pk.type,'auto')
        self.assertEqual(pk.internal_type, None)
        self.assertEqual(pk.python_type, None)
        self.assertEqual(str(pk), 'examples.simplemodel.id')
        self.assertRaises(FieldError, pk.register_with_model,
                          'bla', SimpleModel)
        
    def testCreateWithValue(self):
        # create an instance with an id
        id = self.random_id()
        m1 = SimpleModel(id=id, code='bla').save()
        self.assertEqual(m1.id, id)
        self.assertEqual(m1.code, 'bla')
        m2 = SimpleModel(code='foo').save()
        id2 = self.random_id(id)
        self.assertEqualId(m2, id2)
        self.assertEqual(m2.code, 'foo')
        qs = SimpleModel.objects.query()
        self.assertEqual(qs.count(), 2)
        self.assertEqual(set(qs), set((m1,m2)))
    
    def testCreateWithValue2(self):
        id = self.random_id()
        m1 = SimpleModel(code='bla').save()
        m2 = SimpleModel(id=id, code = 'foo').save()
        self.assertEqualId(m1, 1)
        self.assertEqual(m2.id, id)
        qs = SimpleModel.objects.query()
        self.assertEqual(qs.count(),2)
        self.assertEqual(set(qs), set((m1,m2)))
    
    
class CompositeId(test.CleanTestCase):
    multipledb = 'redis'
    model = WordBook
    
    def setUp(self):
        self.register()
        
    def testMeta(self):
        id = self.model._meta.pk
        self.assertEqual(id.type,'composite')
        self.assertEqual(id.fields,('word','book'))
    
    def testCreate(self):
        m = self.model(word='hello',book='world').save()
        self.assertEqual(m.id,'word:hello,book:world')
        all = self.model.objects.query().all()
        self.assertEqual(len(all),1)
        m = all[0]
        self.assertEqual(m.word,'hello')
        self.assertEqual(m.book,'world')
        #
        m.word = 'beautiful'
        m.save()
        self.assertEqual(m.id,'word:beautiful,book:world')
        self.assertEqual(self.model.objects.query().count(),1)
        all = self.model.objects.query().all()
        self.assertEqual(len(all),1)
        m = all[0]
        self.assertEqual(m.word,'beautiful')
        self.assertEqual(m.book,'world')
        