'''tests for odm.ListField'''
from stdnet import StructureFieldError
from stdnet.utils import test, zip, to_string

from examples.models import SimpleList

from .struct import MultiFieldMixin

        
class TestListField(MultiFieldMixin, test.TestCase):
    model = SimpleList
    attrname = 'names'
        
    def adddata(self, li):
        '''Add elements to a list without using transactions.'''
        with li.session.begin():
            names = li.names
            for elem in self.data.names:
                names.push_back(elem)
        self.assertEqual(li.names.size(), len(self.data.names))
        
    def test_push_back_pop_back(self):
        li = SimpleList()
        self.assertEqual(li.id, None)
        li = yield self.session().add(li)
        names = li.names
        for elem in self.data.names:
            names.push_back(elem)
        self.assertEqual(li.names.size(), len(self.data.names))
        for elem in reversed(self.data.names):
            self.assertEqual(li.names.pop_back(), elem)
        self.assertEqual(li.names.size(), 0)
        
    def testPushBack(self):
        models = self.mapper
        li = yield models.simplelist.new()
        with li.session.begin() as t:
            names = li.names
            for elem in self.data.names:
                names.push_back(elem)
        yield t.on_result
        for el, ne in zip(self.data.names, names):
            self.assertEqual(el, ne)
        self.assertEqual(li.names.size(), len(self.data.names))
        
    def testPushNoSave(self):
        '''Push a new value to a list field should rise an error if the object
is not saved on databse.'''
        obj = self.model()
        push_back  = lambda : obj.names.push_back('this should fail')
        push_front = lambda : obj.names.push_front('this should also fail')
        self.assertRaises(StructureFieldError, push_back)
        self.assertRaises(StructureFieldError, push_front)
        
    def testPushFront(self):
        li = yield self.session().add(SimpleList())
        if li.session.backend.name == 'redis':
            names = li.names
            for elem in reversed(self.data.names):
                names.push_front(elem)
            li.save()
            for el, ne in zip(self.data.names, names):
                self.assertEqual(el, ne)
        
    def testPushFrontPopFront(self):
        li = yield self.session().add(SimpleList())
        if li.session.backend.name == 'redis':
            names = li.names
            for elem in reversed(self.data.names):
                names.push_front(elem)
            li.save()
            self.assertEqual(li.names.size(),len(self.data.names))
            for elem in self.data.names:
                self.assertEqual(li.names.pop_front(), elem)
            self.assertEqual(li.names.size(), 0)