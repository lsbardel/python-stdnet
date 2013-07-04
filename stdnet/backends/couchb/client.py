import stdnet

try:
    import requests
except ImportError:
    requests = None
try:
    from pulsar.apps.http import HttpClient
except ImportError:
    HttpClient = None
    

headers = {'Accept': '*/*',
           'content-type': 'application/json',
           'user-agent': 'stdnet-%s' % stdnet.__version__}
error_classes = {}

class CouchDbError(Exception):
    
    def __init__(self, error, reason):
        self.error = error
        super(CouchDbError, self).__init__(reason)
        
        
class CouchDbNoDbError(CouchDbError):
    pass


error_classes['no_db_file'] = CouchDbNoDbError 
        
class CouchDb(object):
    
    def __init__(self, address, timeout=None, **params):
        self.address = address
        if HttpClient:
            self.http = PulsarHttp(address, timeout)
        else:
            raise NotImplementedError
    
    # DOCUMENTS API
    
    def get(self, db, id, **kwargs):
        '''retrieve an existing document from database ``db`` by ``id``.'''
        return self.http.request('get', db, id, data=kwargs)
    
    def put(self, db, document):
        '''Update a document'''
        return self.http.get('%s/%s/%s' % (self.address, db, id), data=kwargs)
    
    def post(self, db, document):
        return self.http.request('post', db, data=document)
    
    # SERVER API
    
    def info(self):
        return self.http.request('get')
    
    # DATABASE API
    
    def databases(self):
        '''Return a list of all databases in'''
        return self.http.request('get', '_all_dbs')
    
    def createdb(self, dbname):
        '''Create a new database ``dbname``.'''
        return self.http.request('put', dbname)
    
    def deletedb(self, dbname):
        '''Delete an existing database ``dbname``.'''
        return self.http.request('delete', dbname)
        
        
def couch_db_error(error=None, reason=None, **params):
    error_class = error_classes.get(reason, CouchDbError)
    raise error_class(error, reason)
    
    
class PulsarHttp:
    
    def __init__(self, address, timeout=0):
        force_sync = timeout is not 0
        self.address = address
        self.http = HttpClient(headers=headers, force_sync=force_sync)
        
    def request(self, method, *bits, **kwargs):
        url = '%s/%s' % (self.address, '/'.join(bits))
        response = yield self.http.request(method, url, data=kwargs).on_finished
        data = response.content_json()
        if 'error' in data:
            raise couch_db_error(**data)
        else:
            yield data
        