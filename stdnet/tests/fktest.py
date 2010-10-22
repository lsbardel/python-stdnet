import datetime
import random
from itertools import izip

from stdnet.test import TestCase

from examples.models import Node

STEPS   = 10

class TestSelfForeignKey(TestCase):
        
    def create(self, N, root):
        for n in range(N):
            node = Node(parent = root, weight = random.uniform(0,1)).save()
            
    def setUp(self):
        self.orm.register(Node)
        root = Node(weight = 1.0).save()
        for n in range(STEPS):
            node = Node(parent = root, weight = random.uniform(0,1)).save()
            self.create(random.randint(0,9), node)
            
    def unregister(self):
        self.orm.unregister(Node)
    
    def testSelfRelated(self):
        root = Node.objects.filter(parent = None)
        self.assertEqual(len(root),1)
        root = root[0]
        children = list(root.children.all())
        self.assertEqual(len(children),STEPS)
        for child in children:
            self.assertEqual(child.parent,root)
            
        