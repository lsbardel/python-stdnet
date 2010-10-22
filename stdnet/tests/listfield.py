from copy import copy
from itertools import izip

from stdnet import FieldError
from stdnet.test import TestCase
from stdnet.utils import populate

from examples.models import SimpleList

elems = populate('string', 200)


class TestLListField(TestCase):
    
    def setUp(self):
        self.orm.register(SimpleList)
        
    def testPushBackPopBack(self):
        li = SimpleList()
        self.assertEqual(li.id,None)
        li.save()
        names = li.names
        for elem in elems:
            names.push_back(elem)
        li.save()
        self.assertEqual(li.names.size(),len(elems))
        for elem in reversed(elems):
            self.assertEqual(li.names.pop_back(),elem)
        self.assertEqual(li.names.size(),0)
    
    def testPushFrontPopFront(self):
        li = SimpleList().save()
        names = li.names
        for elem in reversed(elems):
            names.push_front(elem)
        li.save()
        self.assertEqual(li.names.size(),len(elems))
        for elem in elems:
            self.assertEqual(li.names.pop_front(),elem)
        self.assertEqual(li.names.size(),0)
        
    def testPushBack(self):
        li = SimpleList().save()
        names = li.names
        for elem in elems:
            names.push_back(elem)
        li.save()
        for el,ne in izip(elems,names):
            self.assertEqual(el,ne)
            
    def testPushFront(self):
        li = SimpleList().save()
        names = li.names
        for elem in reversed(elems):
            names.push_front(elem)
        li.save()
        for el,ne in izip(elems,names):
            self.assertEqual(el,ne)

