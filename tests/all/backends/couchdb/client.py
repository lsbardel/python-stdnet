'''Test CouchDB client.'''
from stdnet.utils import test
from stdnet.backends.couchb import CouchDbError, CouchDbNoDbError 


class TestCase(test.TestWrite):
    multipledb = 'couch'
    
    def setUp(self):
        self.client = self.backend.client
        
    def test_info(self):
        result = yield self.client.info()
        self.assertTrue('version' in result)
    
    def test_createdb(self):
        name = self.name('test')
        result = yield self.client.createdb(name)
        self.assertTrue(result['ok'])
        
    def test_createdb_illegal(self):
        yield self.async.assertRaises(CouchDbError,
                                      self.client.createdb, 'bla.foo')
        
    # Documents
    
    def test_get_invalid_document(self):
        yield self.async.assertRaises(CouchDbNoDbError,
                                      self.client.get, 'bla', '234234')
    
    def test_create_document(self):
        name = self.name('test1')
        yield self.client.createdb(name)
        result = yield self.client.post(name, {'title': 'Hello World',
                                               'author': 'lsbardel'})
        self.assertTrue(result['ok'])
        id = result['id']
        doc = yield self.client.get(name, result['id'])
        data = doc['data']
        self.assertEqual(data['author'], 'lsbardel')
        self.assertEqual(data['title'], 'Hello World')
            
    def name(self, name):
        return self.backend.namespace + 'test'