'''tests for odm.ListField'''
from stdnet import StructureFieldError
from stdnet.utils import test, populate, zip, iteritems, to_string

from examples.models import SimpleList

from .struct import MultiFieldMixin, elems

        
class TestListField(test.TestCase, MultiFieldMixin):
    model = SimpleList
    attrname = 'names'
    
    @classmethod
    def setUpClass(cls):
        yield super(TestListField, cls).setUpClass()
        cls.register()
        
    def adddata(self, li):
        '''Add elements to a list without using transactions.'''
        with li.session.begin():
            names = li.names
            for elem in elems:
                names.push_back(elem)
        self.assertEqual(li.names.size(), len(elems))
        
    def test_push_back_pop_back(self):
        li = SimpleList()
        self.assertEqual(li.id, None)
        yield li.save()
        names = li.names
        for elem in elems:
            names.push_back(elem)
        self.assertEqual(li.names.size(), len(elems))
        for elem in reversed(elems):
            self.assertEqual(li.names.pop_back(), elem)
        self.assertEqual(li.names.size(), 0)
        
    def testPushBack(self):
        li = SimpleList().save()
        with li.session.begin():
            names = li.names
            for elem in elems:
                names.push_back(elem)
        for el, ne in zip(elems, names):
            self.assertEqual(el, ne)
        self.assertEqual(li.names.size(), len(elems))
        
    def testPushNoSave(self):
        '''Push a new value to a list field should rise an error if the object
is not saved on databse.'''
        obj = self.model()
        push_back  = lambda : obj.names.push_back('this should fail')
        push_front = lambda : obj.names.push_front('this should also fail')
        self.assertRaises(StructureFieldError, push_back)
        self.assertRaises(StructureFieldError, push_front)
        
    def testPushFront(self):
        li = SimpleList().save()
        if li.session.backend.name == 'redis':
            names = li.names
            for elem in reversed(elems):
                names.push_front(elem)
            li.save()
            for el,ne in zip(elems,names):
                self.assertEqual(el,ne)
        
    def testPushFrontPopFront(self):
        li = SimpleList().save()
        if li.session.backend.name == 'redis':
            names = li.names
            for elem in reversed(elems):
                names.push_front(elem)
            li.save()
            self.assertEqual(li.names.size(),len(elems))
            for elem in elems:
                self.assertEqual(li.names.pop_front(),elem)
            self.assertEqual(li.names.size(),0)