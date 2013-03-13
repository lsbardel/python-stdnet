'''tests for odm.StringField'''
from stdnet.utils import test, populate, zip, iteritems, to_string

from examples.models import SimpleString

from .struct import MultiFieldMixin, elems


class TestStringField(test.TestCase, MultiFieldMixin):
    multipledb = 'redis'
    model = SimpleString
    
    @classmethod
    def setUpClass(cls):
        yield super(TestStringField, cls).setUpClass()
        cls.register()
        
    def adddata(self, li):
        '''Add elements to a list without using transactions.'''
        for elem in elems:
            li.data.push_back(elem)
        self.assertEqual(li.data.size(),len(''.join(elems)))
        
    def test_incr(self):
        m = yield self.model().save()
        self.async.assertEqual(m.data.incr(), 1)
        self.async.assertEqual(m.data.incr(), 2)
        self.async.assertEqual(m.data.incr(3), 5)
        self.async.assertEqual(m.data.incr(-7), -2)
        

        