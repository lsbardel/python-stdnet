from stdnet.utils import test, populate, zip, iteritems, to_string

from examples.models import Dictionary

from .struct import MultiFieldMixin
        

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
        for m in self.model.objects.query():
            data = getattr(m,cache,None)
            self.assertFalse(data)
        
    def test_load_related(self):
        '''Use load_selected to load stastructure data'''
        cache = self.model._meta.dfields['data'].get_cache_name()
        for m in self.model.objects.query().load_related('data'):
            self.assertTrue(m.data.cache.cache)
        
