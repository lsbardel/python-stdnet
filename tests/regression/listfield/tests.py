from copy import copy
from time import sleep

from stdnet import test, MultiFieldError
from stdnet.utils import populate, zip

from examples.models import SimpleList

elems = populate('string', 100)


class BaseTestListField(test.TestCase,test.TestMultiFieldMixin):
    
    def get_object_and_field(self):
        d = SimpleList().save()
        return d,d.names
    
    def adddata(self,li):
        names = li.names
        for elem in elems:
            names.push_back(elem)
        li.save()
        self.assertEqual(li.names.size(),len(elems))
    
    def setUp(self):
        self.orm.register(SimpleList)
        
    def unregister(self):
        self.orm.unregister(SimpleList)


class TestListField(BaseTestListField):
    '''Test ListField'''        
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
        for el,ne in zip(elems,names):
            self.assertEqual(el,ne)
            
    def testPushFront(self):
        li = SimpleList().save()
        names = li.names
        for elem in reversed(elems):
            names.push_front(elem)
        li.save()
        for el,ne in zip(elems,names):
            self.assertEqual(el,ne)


class TestErrorListFields(BaseTestListField):
    
    def testPushNoSave(self):
        '''Push a new value to a list field should rise an error if the object is not
saved on databse.'''
        obj = SimpleList()
        push_back  = lambda : obj.names.push_back('this should fail')
        push_front = lambda : obj.names.push_front('this should also fail')
        self.assertRaises(MultiFieldError,push_back)
        self.assertRaises(MultiFieldError,push_front)
        

class TestTimeOutListField(BaseTestListField):
    
    def setUp(self):
        self.orm.register(SimpleList, timeout = 1)
        
    def __testTimeout(self):
        s1 = SimpleList().save()
        self.assertEqual(SimpleList.objects.all().count(),1)
        sleep(2)
        self.assertEqual(SimpleList.objects.all().count(),0)
        
