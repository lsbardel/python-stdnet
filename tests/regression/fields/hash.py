from stdnet import test
from stdnet.utils import populate, zip, iteritems, to_string

from examples.models import Dictionary

keys = populate('string', 200)
values = populate('string', 200, min_len = 20, max_len = 300)


class TestHashField(test.TestCase,test.TestMultiFieldMixin):
    model = Dictionary
    
    def setUp(self):
        self.register()
        d = Dictionary(name = 'test').save()
        self.data = dict(zip(keys,values))
        
    def get_object_and_field(self):
        d = Dictionary.objects.get(name = 'test')
        return d,d.data
    
    def adddata(self,d):
        d.data.update(self.data)
        self.assertEqual(d.data.size(),0)
        d.save()
        data = d.data
        self.assertEqual(data.size(),len(self.data))
    
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
        

class TestMultiField(test.TestCase):
    model = Dictionary
    
    def setUp(self):
        self.register()
        m = self.model(name = 'bla').save()
        m.data['ciao'] = 'bla'
        m.data['hello'] = 'foo'
        m.data['hi'] = 'pippo'
        m.data['salut'] = 'luna'
        m.save()
        m = self.model(name = 'luca').save()
        m.data['hi'] = 'pippo'
        m.data['salut'] = 'luna'
        m.save()
        
    def testloadNotSelected(self):
        '''Get the model and check that no data-structure data
 has been loaded.'''
        cache = self.model._meta.dfields['data'].get_cache_name()
        for m in self.model.objects.all():
            data = getattr(m,cache,None)
            self.assertFalse(data)
        
    def testloadselected(self):
        '''Use load_selected to load stastructure data'''
        cache = self.model._meta.dfields['data'].get_cache_name()
        for m in self.model.objects.all().load_related():
            data = getattr(m,cache,None)
            self.assertTrue(data)
            self.assertTrue(data.cache)
        
