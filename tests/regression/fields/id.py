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
        return Task(id = genid()).save()
    
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
        
    def test_save_as_new(self):
        t1 = self.make()
        id1 = t1.id
        ts1 = t1.timestamp
        time.sleep(0.01)
        t2 = t1.save_as_new(id = genid())
        self.assertEqual(t1.id,t2.id)
        self.assertEqual(t1.timestamp,t2.timestamp)
        self.assertTrue(ts1<t2.timestamp)
        tasks = list(Task.objects.all())
        self.assertEqual(len(tasks),2)
        self.assertEqual(tasks[0].id,t2.id)
        self.assertEqual(tasks[1].id,id1)
        self.assertTrue(tasks[0].timestamp>tasks[1].timestamp)
        
    def test_delete_and_save_as_new(self):
        t1 = self.make()
        t1 = Task.objects.get(id = t1.id)
        t1.delete()
        t1.save_as_new(id = genid())
        tasks = list(Task.objects.all())
        self.assertEqual(len(tasks),1)
        self.assertEqual(tasks[0].id,t1.id)
