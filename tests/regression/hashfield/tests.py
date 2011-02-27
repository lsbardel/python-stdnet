from stdnet import test
from stdnet.utils import populate, zip, iteritems, to_string

from examples.models import Dictionary

keys = populate('string', 200)
values = populate('string', 200, min_len = 20, max_len = 300)


class TestHashField(test.TestCase,test.TestMultiFieldMixin):
    
    def get_object_and_field(self):
        d = Dictionary.objects.get(name = 'test')
        return d,d.data
    
    def adddata(self,d):
        data = d.data
        d.data.update(self.data)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(self.data))
    
    def setUp(self):
        self.orm.register(Dictionary)
        d = Dictionary(name = 'test').save()
        self.data = dict(zip(keys,values))
        
    def unregister(self):
        self.orm.unregister(Dictionary)
    
    def fill(self):
        d = Dictionary.objects.get(name = 'test')
        self.adddata(d)
        return Dictionary.objects.get(name = 'test')
    
    def testUpdate(self):
        self.fill()
    
    def testAdd(self):
        d = Dictionary.objects.get(name = 'test')
        for k,v in iteritems(self.data):
            d.data.add(k,v)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        
    def testKeys(self):
        d = self.fill()
        for k in d.data.keys():
            k = to_string(k)
            self.data.pop(k)
        self.assertEqual(len(self.data),0)
    
    def testItems(self):
        d = self.fill()
        for k,v in d.data.items():
            k = to_string(k)
            self.assertEqual(v,self.data.pop(k))
        self.assertEqual(len(self.data),0)
        
