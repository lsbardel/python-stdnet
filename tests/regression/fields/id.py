'''ID with AutoField and custom.'''
from uuid import uuid4
import time

import stdnet
from stdnet import test

from examples.models import Task


def genid():
    return str(uuid4())[:8]


class Id(test.TestCase):
    model = Task
    
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
        self.assertEqual(t1.id,t1._dbdata['id'])
        t1.id = genid()
        self.assertRaises(ValueError,t1.save)
        
    def test_clone(self):
        t1 = self.make()
        time.sleep(0.01)
        t2 = t1.clone(id = genid()).save()
        self.assertNotEqual(t1.id,t2.id)
        self.assertEqual(t1.name,t2.name)
        self.assertNotEqual(t1.timestamp,t2.timestamp)
        self.assertTrue(t1.timestamp<t2.timestamp)
        tasks = list(Task.objects.all())
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
        tasks = list(Task.objects.all())
        self.assertEqual(len(tasks),1)
        self.assertEqual(tasks[0].id,t2.id)
