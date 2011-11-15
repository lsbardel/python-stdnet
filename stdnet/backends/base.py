import json
from copy import deepcopy
from hashlib import sha1

from stdnet.exceptions import *
from stdnet.utils import zip, iteritems, itervalues, BytesIO, encoders

from .structures import Structure

__all__ = ['BackendDataServer', 'BeckendQuery']

def intid(id):
    try:
        return int(id)
    except ValueError:
        return id
        
class Keys(object):
    
    def __init__(self,id,timeout,pipeline):
        self.timeout = timeout
        self.value = None
        pipeline[id] = self
        
    def add(self, value):
        self.value = value
        
    
class BeckendQuery(object):
    query_set = None
    
    def __init__(self, qs, fargs = None, eargs = None,
                 timeout = 0, queries = None):
        self.qs = qs
        self.expire = max(timeout,30)
        self.timeout = timeout
        self._sha = BytesIO()
        self.build(fargs, eargs, queries)
        code = self._sha.getvalue()
        self._sha = code if not code else sha1(code).hexdigest()
        self.execute()

    @property
    def meta(self):
        return self.qs._meta
    
    @property
    def backend(self):
        return self.qs.backend
    
    @property
    def sha(self):
        return self._sha
    
    def __len__(self):
        return self.count()

    def has(self, val):
        raise NotImplementedError
    
    def count(self):
        raise NotImplementedError
    
    def build(self, fargs, eargs, queries):
        raise NotImplementedError
    
    def execute(self):
        raise NotImplementedError
    
    def items(self, slic):
        raise NotImplementedError
    
    def load_related(self, result):
        '''load related fields into the query result.

:parameter result: a result from a queryset.
:rtype: the same queryset qith related models loaded.'''
        if self.qs._select_related:
            if not hasattr(result,'__len__'):
                result = list(result)
            meta = self.meta
            for field in self.qs._select_related:
                name = field.name
                attname = field.attname
                vals = [getattr(r,attname) for r in result]
                if field in meta.scalarfields:
                    related = field.relmodel.objects.filter(id__in = vals)
                    for r,val in zip(result,related):
                        setattr(r,name,val)
                else:
                    with meta.model.transaction() as t:
                        for val in vals:
                            val.reload(t)
                    for val,r in zip(vals,t.get_result()):
                        val.set_cache(r)
                        
        return result
    

class BackendDataServer(object):
    '''\
Generic interface for a backend database:
    
:parameter name: name of database, such as **redis**, **couchdb**, etc..
:parameter params: dictionary of configuration parameters
:parameter pickler: calss for serializing and unserializing data.

It must implement the *loads* and *dumps* methods.'''
    Transaction = None
    Query = None
    structure_module = None
    
    def __init__(self, name, address, pickler = None,
                 charset = 'utf-8', connection_string = '',
                 **params):
        self.__name = name
        self._cachepipe = {}
        self._keys = {}
        self.charset = charset
        self.pickler = pickler or encoders.NoEncoder()
        self.connection_string = connection_string
        self.params = params
        self.client = self.setup_connection(address, **params)
        
    def setup_connection(self, address, **params):
        raise NotImplementedError

    @property
    def name(self):
        return self.__name
    
    def __ne__(self, other):
        return not self == other
    
    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return self.issame(other)
        else:
            return False
        
    def issame(self, other):
        return False
    
    def cursor(self, pipelined = False):
        return self
    
    def disconnect(self):
        '''Disconnect the connection.'''
        pass
    
    def __repr__(self):
        return '%s backend' % self.__name
    
    def __str__(self):
        return self.__repr__()
    
    def createdb(self, name):
        pass
    
    def isempty(self):
        '''Returns ``True`` if the database has no keys.'''
        keys = self.keys()
        if not hasattr(keys,'__len__'):
            keys = list(keys)
        return len(keys)
    
    def instance_keys(self, obj):
        '''Return a list of database keys used by instance *obj*'''
        raise NotImplementedError
    
    def transaction(self, pipelined = True, **kwargs):
        '''Return a transaction instance'''
        return self.Transaction(self,self.cursor(pipelined),**kwargs)
        
    def delete_object(self, obj, transaction = None):
        '''Delete an object from the data server and clean up indices.
Called to clear a model instance.

:parameter obj: instance of :class:`stdnet.orm.StdModel`
:parameter deleted: a list or ``None``. If a list, deleted keys
                    will be appended to it.
:parameter multi_field: if ``True`` the multifield ids (if any)
                        will be removed. Default ``True``.
        '''
        if 'id' not in obj._dbdata:
            return 0
        
        commit = False
        if not transaction:
            commit = True
            transaction = self.transaction()
        
        self._delete_object(obj, transaction)
        
        if commit:
            transaction.commit()
            
        return 1
    
    def make_objects(self, meta, ids, data = None, loadedfields = None,
                     loadedfields_attributes = None):
        '''Generator of :class:`stdnet.orm.StdModel` instances with data
from database.

:parameter meta: instance of model :class:`stdnet.orm.base.Metaclass`.
:parameter ids: Iterator over ids.
'''
        make_object = meta.maker
        if data is None:
            for id in ids:
                obj = make_object()
                obj.__setstate__((id,(),{'__dbdata__': {}}))
                obj._dbdata['id'] = obj.id
                yield obj
        else:
            loadedfields = tuple(loadedfields) if loadedfields else None 
            for id,fields in zip(ids,data):
                obj = make_object()
                if loadedfields:
                    fields = dict(zip(loadedfields_attributes,fields))
                else:
                    fields = dict(fields)
                fields['__dbdata__'] = deepcopy(fields)
                obj.__setstate__((id,loadedfields,fields))
                obj._dbdata['id'] = obj.id
                yield obj
        
    def set(self, id, value, timeout = None):
        timeout = timeout or 0
        value = self.pickler.dumps(value)
        return self._set(id,value,timeout)
    
    def get(self, id, default = None):
        v = self._get(id)
        if v:
            return self.pickler.loads(v)
        else:
            return default

    def get_many(self, keys):
        """
        Fetch a bunch of keys from the cache. For certain backends (memcached,
        pgsql) this can be *much* faster when fetching multiple values.

        Returns a dict mapping each key in keys to its value. If the given
        key is missing, it will be missing from the response dict.
        """
        d = {}
        for k in keys:
            val = self.get(k)
            if val is not None:
                d[k] = val
        return d

    def has_key(self, key):
        """
        Returns True if the key is in the cache and has not expired.
        """
        return self.get(key) is not None

    def incr(self, key, delta=1):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        if key not in self:
            raise ValueError("Key '%s' not found" % key)
        new_value = self.get(key) + delta
        self.set(key, new_value)
        return new_value

    def decr(self, key, delta=1):
        """Subtract delta from value in the cache.
If the key does not exist, raise a ValueError exception."""
        return self.incr(key, -delta)

    def __contains__(self, key):
        """
        Returns True if the key is in the cache and has not expired.
        """
        # This is a separate method, rather than just a copy of has_key(),
        # so that it always has the same functionality as has_key(), even
        # if a subclass overrides it.
        return self.has_key(key)

    def delete_many(self, keys):
        """
        Set a bunch of values in the cache at once.  For certain backends
        (memcached), this is much more efficient than calling delete() multiple
        times.
        """
        for key in keys:
            self.delete(key)

    # PURE VIRTUAL METHODS
    
    def clear(self):
        """Remove *all* values from the database at once."""
        raise NotImplementedError
    
    def autoid(self, meta):
        raise NotImplementedError
    
    def save_object(self, obj, idnew, transaction):
        raise NotImplementedError
    
    def _remove_indexes(self, obj, transaction):
        raise NotImplementedError
    
    def _delete_object(self, obj, deleted, transaction):
        raise NotImplementedError
    
    def keys(self, pattern = '*'):
        raise NotImplementedError
        
    def _set(self, id, value, timeout):
        raise NotImplementedError
    
    def _get(self, id):
        raise NotImplementedError
    
    def _set_keys(self):
        raise NotImplementedError
    
    def flush(self, meta):
        raise NotImplementedError
            
    # DATASTRUCTURES
    
    def index_keys(self, id, timeout, transaction = None):
        return Keys(id,timeout,self._keys)
    
    def list(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.List`
for a given *id*.'''
        return self.structure_module.List(self, id, 'list',
                                          timeout = timeout, **kwargs)
    
    def hash(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.HashTable` structure
for a given *id*.'''
        return self.structure_module.HashTable(self, id, 'hash',
                                               timeout = timeout, **kwargs)
    
    def ts(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.HashTable` structure
for a given *id*.'''
        return self.structure_module.TS(self, id, 'ts',
                                        timeout = timeout, **kwargs)
    
    def unordered_set(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.Set` structure
for a given *id*.'''
        return self.structure_module.Set(self, id, 'unordered_set',
                                         timeout = timeout, **kwargs)
    
    def ordered_set(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.OrderedSet` structure
for a given *id*.'''
        return self.structure_module.OrderedSet(self, id, 'ordered_set',
                                                timeout = timeout, **kwargs)
    

