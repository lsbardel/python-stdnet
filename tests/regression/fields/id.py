'''ID with AutoField and custom.'''
from uuid import uuid4
import time

import stdnet
from stdnet import test

from examples.models import Task, WordBook


def genid():
    return str(uuid4())[:8]


class Id(test.TestCase):
    '''Test primary key when it is not an AutoField.
Use the manager for convenience.'''
    model = Task
    
    def setUp(self):
        self.register()
        
    def make(self):
        return Task(id = genid(), name = 'pluto').save()
    
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
        self.assertEqual(self.model.objects.query().count(),2)
        
    def test_clone(self):
        t1 = self.make()
        time.sleep(0.01)
        t2 = t1.clone(id = genid()).save()
        self.assertNotEqual(t1.id,t2.id)
        self.assertEqual(t1.name,t2.name)
        self.assertNotEqual(t1.timestamp,t2.timestamp)
        self.assertTrue(t1.timestamp<t2.timestamp)
        tasks = Task.objects.query()
        self.assertEqual(len(tasks),2)
        self.assertEqual(tasks[0].id,t2.id)
        self.assertEqual(tasks[1].id,t1.id)
        self.assertTrue(tasks[0].timestamp>tasks[1].timestamp)
        
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


class CompositeId(test.TestCase):
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
        m.word = 'beautiful'
        m.save()
        self.assertEqual(m.id,'word:beautiful,book:world')
        self.assertEqual(self.model.objects.query().count(),1)
        