import uuid
from stdnet.backends.base import BaseBackend, ImproperlyConfigured, novalue
from stdnet.backends.structures.structcouchdb import List,Set,HashTable,Map


try:
    import couchdb
except:
    raise ImproperlyConfigured("CouchDB backend requires the 'couchdb-python' library. Do easy_install couchdb-python")

ResourceNotFound = couchdb.client.ResourceNotFound
ResourceConflict = couchdb.client.ResourceConflict


class BackEnd(BaseBackend):
    
    def __init__(self, name, server, params):
        super(BackEnd,self).__init__(name,params)
        servs = server.split(':')
        server = servs[0]
        port   = 5984
        if len(server) == 2:
            port = int(servs[1])
        self.params          = params
        self.db              = params.pop('db',None)
        if not self.db:
            raise ImproperlyConfigured('Database not specified')
        self.server         = couchdb.Server('http://%s:%s' % (server, port))
        try:
            cache  = self.server[self.db]
        except ResourceNotFound:
            cache  = self.createdb(self.db)
            raise ImproperlyConfigured('Database %s not available')
        self.db      = cache
        try:
            self.strings = cache['strings']
        except ResourceNotFound:
            cache['strings'] = {}
            self.strings = cache['strings']
            
    
    def createdb(self, name):
        return self.server.create(name)
    
    def clear(self):
        return self.db.delete()
    
    def incr(self, key):
        return uuid.uuid4()
    
    def has_key(self, id):
        return id in self.db
    
    def set(self, id, value, timeout = None):
        try:
            self.strings[id] = value
        except:
            pass
    
    def get(self, id):
        try:
            return self.string[id]
        except ResourceNotFound:
            return None        
        
    def delete(self, *keys):
        km = ()
        for key in keys:
            km += RedisMap1(self,key).ids()
        return self._cache.delete(*km)
    
    def list(self, id, timeout = 0):
        return List(self,id,timeout)
    
    def unordered_set(self, id, timeout = 0):
        return Set(self,id,timeout)
    
    def hash(self, id, timeout = 0):
        return HashTable(self,id,timeout)
    
    def map(self, id, timeout = 0):
        return Map(self,id,timeout)
    
    def _val_to_store_info(self, value):
        return self.pickler.dumps(value)
    
    def _res_to_val(self, res):
        if not res:
            return res
        try:
            return self.pickler.loads(res)
        except:
            return res
    
    # Hashes
    def hset(self, id, key, value):
        value = self._val_to_store_info(value)
        return self.execute_command('HSET', id, key, value)
    
    def hmset(self, id, mapping):
        items = []
        [items.extend((key,self._val_to_store_info(value))) for key,value in mapping.iteritems()]
        return self.execute_command('HMSET', id, *items)
    
    def hget(self, id, key):
        res = self.execute_command('HGET', id, key)
        return self._res_to_val(res)
    
