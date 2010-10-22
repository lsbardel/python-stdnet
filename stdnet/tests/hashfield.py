from itertools import izip

from stdnet.test import TestCase
from stdnet.utils import populate

from examples.models import Dictionary

keys = populate('string', 200)
values = populate('string', 200, min_len = 20, max_len = 300)


class TestLHashField(TestCase):
    
    def setUp(self):
        self.orm.register(Dictionary)
        d = Dictionary(name = 'test').save()
        self.data = dict(izip(keys,values))
    
    def fill(self):
        d = Dictionary.objects.get(name = 'test')
        d.data.update(self.data)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(self.data))
        return Dictionary.objects.get(name = 'test')
    
    def testUpdate(self):
        self.fill()
    
    def testAdd(self):
        d = Dictionary.objects.get(name = 'test')
        for k,v in self.data.iteritems():
            d.data.add(k,v)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        
    def testKeys(self):
        d = self.fill()
        for k in d.data.keys():
            self.data.pop(k)
        self.assertEqual(len(self.data),0)
    
    def testItems(self):
        d = self.fill()
        for k,v in d.data.items():
            self.assertEqual(v,self.data.pop(k))
        self.assertEqual(len(self.data),0)
        
    