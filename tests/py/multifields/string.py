'''tests for odm.StringField'''
from stdnet.utils import test, populate, zip, iteritems, to_string

from examples.models import SimpleString

from .struct import MultiFieldMixin, elems


class TestStringField(test.TestCase, MultiFieldMixin):
    multipledb = 'redis'
    model = SimpleString
    
    def adddata(self, li):
        '''Add elements to a list without using transactions.'''
        for elem in elems:
            li.data.push_back(elem)
        self.assertEqual(li.data.size(),len(''.join(elems)))
        
    def testIncr(self):
        m = self.model().save()
        self.assertEqual(m.data.incr(),1)
        self.assertEqual(m.data.incr(),2)
        self.assertEqual(m.data.incr(3),5)
        self.assertEqual(m.data.incr(-7),-2)
        

        