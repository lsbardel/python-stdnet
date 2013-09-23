'''tests for odm.HashField'''
from stdnet.utils import test, zip, iteritems, to_string

from examples.models import Dictionary

from .struct import MultiFieldMixin
        

class HashData(test.DataGenerator):
    
    def generate(self):
        self.keys = self.populate()
        self.values = self.populate(min_len=20, max_len=300)
        self.data = dict(zip(self.keys, self.values))


class TestHashField(MultiFieldMixin, test.TestCase):
    multipledb = 'redis'
    model = Dictionary
    data_cls = HashData
    
    def defaults(self):
        return {'name': self.name}
    
    def adddata(self, d):
        yield d.data.update(self.data.data)
        size = yield d.data.size()
        self.assertEqual(len(self.data.data), size)
    
    def create(self, fill=False):
        with self.session().begin() as t:
            d = t.add(self.model(name=self.name))
        yield t.on_result
        if fill:
            yield d.data.update(self.data.data)
        yield d
        
    def test_update(self):
        d = yield self.create(True)
        data = d.data
        self.assertTrue(data.field)
        self.assertEqual(data.session, d.session)
        self.assertEqual(data.cache.cache, None)
        items = yield data.items()
        self.assertTrue(data.cache.cache)
        self.assertNotEqual(data.cache.cache, items)
        self.assertEqual(data.cache.cache, dict(items))
    
    def test_add(self):
        d = yield self.create()
        self.assertTrue(d.session)
        with d.session.begin() as t:
            for k, v in iteritems(self.data.data):
                d.data.add(k, v)
            size = yield d.data.size()
            self.assertEqual(size, 0)
        yield t.on_result
        size = yield d.data.size()
        self.assertEqual(len(self.data.data), size)

    def testKeys(self):
        d = yield self.create(True)
        data = self.data.data.copy()
        for k in d.data:
            data.pop(k)
        self.assertEqual(len(data), 0)
    
    def testItems(self):
        d = yield self.create(True)
        data = self.data.data.copy()
        items = d.data.items()
        for k, v in items:
            self.assertEqual(v, data.pop(k))
        self.assertEqual(len(data), 0)
        
    def testValues(self):
        d = yield self.create(True)
        values = yield d.data.values()
        self.assertEqual(len(self.data.data), len(values))
        
    def createN(self):
        with self.session().begin() as t:
            for name in self.names:
                t.add(self.model(name=name))
        yield t.on_result
        # Add some data to dictionaries
        qs = yield self.query().all()
        self.assertTrue(qs)
        with self.session().begin() as t:
            for m in qs:
                t.add(m.data)
                m.data['ciao'] = 'bla'
                m.data['hello'] = 'foo'
                m.data['hi'] = 'pippo'
                m.data['salut'] = 'luna'
        yield t.on_result
        
    def testloadNotSelected(self):
        '''Get the model and check that no data-structure data
 has been loaded.'''
        yield self.createN()
        cache = self.model._meta.dfields['data'].get_cache_name()
        qs = yield self.query().all()
        self.assertTrue(qs)
        for m in qs:
            data = getattr(m, cache, None)
            self.assertFalse(data)
        
    def test_load_related(self):
        '''Use load_selected to load stastructure data'''
        yield self.createN()
        cache = self.model._meta.dfields['data'].get_cache_name()
        all = yield self.query().load_related('data').all()
        for m in all:
            self.assertTrue(m.data.cache.cache)