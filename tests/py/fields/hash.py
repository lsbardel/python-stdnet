from stdnet.utils import test, populate, zip, iteritems, to_string

from examples.models import Dictionary

from .struct import MultiFieldMixin
        
keys = populate('string', 200)
values = populate('string', 200, min_len=20, max_len=300)


class TestMultiField(test.CleanTestCase):
    multipledb = 'redis'
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
        for m in self.model.objects.query():
            data = getattr(m,cache,None)
            self.assertFalse(data)
        
    def test_load_related(self):
        '''Use load_selected to load stastructure data'''
        cache = self.model._meta.dfields['data'].get_cache_name()
        for m in self.model.objects.query().load_related('data'):
            self.assertTrue(m.data.cache.cache)
        

class TestHashField(MultiFieldMixin, test.CleanTestCase):
    multipledb = 'redis'
    model = Dictionary
    defaults = {'name': 'test'}
    
    def data(self):
        self.data = dict(zip(keys, values))
    
    def adddata(self, d):
        self.data()
        d.data.update(self.data)
        self.assertEqual(d.data.size(), len(self.data))
    
    def fill(self):
        d = self.model(name='test').save()
        self.adddata(d)
        return Dictionary.objects.get(name='test')
    
    def testUpdate(self):
        self.fill()
    
    def testAdd(self):
        self.data()
        d = self.model(name='test').save()
        self.assertTrue(d.session)
        self.assertTrue(d in d.session)
        with d.session.begin():
            for k,v in iteritems(self.data):
                d.data.add(k, v)
            self.assertEqual(d.data.size(), 0)
        self.assertTrue(d.data.size(), 0)
        
    def testKeys(self):
        d = self.fill()
        for k in d.data:
            self.data.pop(k)
        self.assertEqual(len(self.data),0)
    
    def testItems(self):
        d = self.fill()
        for k, v in d.data.items():
            self.assertEqual(v, self.data.pop(k))
        self.assertEqual(len(self.data), 0)
        
    def testValues(self):
        d = self.fill()
        values = list(d.data.values())
        self.assertEqual(len(self.data),len(values))