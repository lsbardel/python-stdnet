'''MongoDB backend implementation. Requires pymongo_.

.. _pymongo: http://api.mongodb.org/python/current/
'''
import json
from itertools import chain
try:
    import pymongo
except ImportError:
    raise ImportError('Mongo backend requires pymongo')

import stdnet
from stdnet import FieldValueError, QuerySetError
from stdnet.utils import async, unique_tuple
from stdnet.backends import instance_session_result


def extend_dict(iterable):
    d = {}
    for name, value in iterable:
        if not isinstance(value, list):
            value = [value]
        if name in d:
            d[name].extend(value)
        else:
            d[name] = value
    return d

################################################################################
##    MONGODB QUERY CLASS
################################################################################
def ids_from_query(query):
    return [v['_id'] for v in query.find(fields=['_id'])]
    
    
class MongoDbQuery(async.BackendQuery):
    '''Backend query implementation for MongoDB.
For detail information about mongo query selectors check the online
documentation http://docs.mongodb.org/manual/reference/operators/.'''
    selector_map = {'gt': '$gt',
                    'lt': '$lt',
                    'ge': '$gte',
                    'le': '$lte'}
    
    def _build(self):
        self.spec = self._unwind(self.queryelem)
        where = self.queryelem.data.get('where')
        if where:
            self.spec['$where'] = where[0]
        
    def find(self, **params):
        collection = self.backend.collection(self.queryelem.meta)
        return collection.find(self.spec, **params)
        
    def remove(self, commands=None):
        collection = self.backend.collection(self.queryelem.meta)
        if commands:
            commands.append(('remove', self.spec))
        return collection.remove(self.spec)
            
    def _unwind(self, queryelem, selector='$in'):
        keyword = queryelem.keyword
        pkname = queryelem.meta.pkname()
        data = {}
        if keyword == 'union':
            if selector == '$in':
                logical = '$or'
            elif selector == '$nin':
                logical = '$nor'
            return {logical: list(self._logical(queryelem, selector))}
        elif keyword == 'intersection':
            return {'$and': list(self._logical(queryelem, selector))}
        elif keyword == 'diff':
            return self._accumulate(self._selectors(queryelem, selector,'$nin'))
        else:
            return self._accumulate(self._selectors(queryelem, selector))
    
    def _logical(self, queryelem, selector):
        for child in queryelem:
            yield self._accumulate(self._selectors(child, selector))
            
    def _selectors(self, queryelem, selector, selector2=None):
        pkname = queryelem.meta.pkname()
        name = queryelem.name
        for child in queryelem:
            if getattr(child, 'backend', None) is not None:
                lookup, value = 'set', child
            else:
                lookup, value = child
            if name == pkname:
                name = '_id'
            if lookup == 'set':
                if value.meta != queryelem.meta:
                    qs = self.__class__(value)
                    yield name, selector, ids_from_query(qs)
                else:
                    if name == '_id' and not value.underlying:
                        continue
                    else:
                        for n, sel, value in self._selectors(value, selector):
                            yield n, sel, value
            else:
                if lookup == 'value':
                    sel = selector
                else:
                    sel = self.selector_map[lookup]
                yield name, sel, value
            selector = selector2 or selector
    
    def _accumulate(self, data):
        kwargs = {}
        for name, selector, value in data:
            if name in kwargs:
                data = kwargs[name]
                if selector in ('$in', '$nin'):
                    if selector in data:
                        data[selector].append(value)
                    else:
                        data[selector] = [value]
                else:
                    data[selector] = value
            else:
                if selector in ('$in', '$nin') and not isinstance(value, list):
                    value = [value]
                kwargs[name] = {selector: value}
        return kwargs
    
    def _execute_query(self):
        collection = self.backend.collection(self.queryelem.meta)
        return collection.find(self.spec).count()
        
    def _items(self, slic):
        meta = self.meta
        get = self.queryelem._get_field
        fields_attributes = None
        pkname_tuple = (meta.pk.name,)
        if get:
            raise QuerySetError('Not implemented')
        else:
            fields = self.queryelem.fields or None
            if fields:
                fields = unique_tuple(fields, self.queryelem.select_related or ())
            if fields == pkname_tuple:
                fields_attributes = fields
            elif fields:
                fields, fields_attributes = meta.backend_fields(fields)
        query = self.find(fields=fields_attributes)
        data = self.backend.build(query, meta, fields, fields_attributes)
        return self.backend.objects_from_db(meta, data)
            

################################################################################
##    MONGODB STRUCTURES
################################################################################
size_script = '\n'.join(('var doc = db.%s.findOne({"_id": ObjectId("%s")});',
                         'if (doc) {',
                         '    var field = doc.%s;',
                         '    if (field) {',
                         '        return field.length;',
                         '    }',
                         '}',
                         'return 0;'))
                         
class MongoStructure(stdnet.BackendStructure):
    
    def __init__(self, *args, **kwargs):
        super(MongoStructure, self).__init__(*args, **kwargs)
        instance = self.instance
        field = instance.field
        if field:
            model = field.model 
            if instance._pkvalue:
                id = self.backend.basekey(model._meta, 'obj', instance._pkvalue)
            else:
                raise NotImplemenetedError('Class structures not available')
        else:
            raise NotImplemenetedError('Stand alone structures not available')
        self.name = field.name
        self.id = id
        
    def selector(self):
        if self.instance.instance is not None:
            return {'_id': self.instance.instance.id}
        else:
            raise NotImplementedError('Mongodb can have structures attached to'
                                      ' objects only')
    
    @property
    def document(self):
        return self.instance.instance
    
    @property
    def collection_name(self):
        return self.backend.basekey(self.instance.instance._meta)
        
    @property
    def collection(self):
        return getattr(self.backend.db, self.collection_name)
        
    def __iter__(self):
        name = self.name
        result = self.collection.find_one(self.selector(), fields=(name,))
        return iter(result.get(name, ()))
            
    def delete(self):
        return self.client.delete(self.id)
    
    def size(self):
        instance = self.instance.instance
        collection = self.collection_name
        func = size_script % (collection, instance.pkvalue(), self.name)
        return int(self.backend.db.eval(func))
    
    
class List(MongoStructure):
    
    def pop_front(self):
        result = self.collection.find_and_modify(query=self.selector(),
                                                update={'$pop': {self.name: 1}})
        return result[self.name][0]
    
    def pop_back(self):
        result = self.collection.find_and_modify(query=self.selector(),
                                                update={'$pop': {self.name: 1}})
        return result[self.name][-1]
    
    def flush(self):
        cache = self.instance.cache
        if cache.front or cache.back:
            selector = [self.selector()]
            name = self.name
            if cache.back:
                selector.append({'$pushAll': {name: cache.back}})
            if cache.front:
                raise NotImplementedError('Mongodb cannot prepend values to'
                                          ' a list field.')
            return selector
    
    
class Set(MongoStructure):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            selector = [self.selector()]
            selector.append({'$addToSet':
                             { self.name: {'$each': list(cache.toadd)}}})
            return selector
    
    
################################################################################
##    MONGODB BACKEND
################################################################################
class BackendDataServer(stdnet.BackendDataServer):
    Query = MongoDbQuery
    default_port = 27017
    struct_map = {'list': List, 'set': Set}
        
    def setup_connection(self, address):
        if len(address) == 1:
            address.append(self.default_port)
        db = ('%s%s' % (self.params.pop('db', ''), self.namespace))
        self.namespace = ''
        mdb = pymongo.MongoClient(address[0], int(address[1]), **self.params)
        self.params['db'] = db.replace('.','') or 'test'
        return mdb
    
    @property
    def db(self):
        return getattr(self.client, self.params['db'])
    
    def collection(self, meta):
        return getattr(self.db, self.basekey(meta))
    
    def model_keys(self, meta):
        return ids_from_query(self.collection(meta))
    
    def setup_model(self, meta):
        for index in getattr(meta, 'indices', ()):
            self.collection(meta).ensure_index(index.name,
                                               background=True,
                                               unique=index.unique)
            
    def flush(self, meta=None):
        '''Flush all model keys from the for a pattern'''
        if meta is not None:
            return self.db.drop_collection(self.basekey(meta))
        else:
            return self.client.drop_database(self.params['db'])
        
    def execute_session(self, session, callback):
        '''Execute a session in mongo.'''
        results = []
        commands = []
        for sm in session:
            meta = sm.meta
            model_type = meta.model._model_type
            if model_type == 'structure':
                self.flush_structure(sm, commands)
            elif model_type == 'object':
                delquery = sm.get_delete_query()
                results.extend(self.delete_query(delquery, commands))
                collection = self.collection(meta)
                instances = []
                processed = []
                modified = []
                for instance in sm.iterdirty():
                    if not instance.is_valid():
                        raise FieldValueError(
                                    json.dumps(instance._dbdata['errors']))
                    state = instance.get_state()
                    data = instance._dbdata['cleaned_data']
                    processed.append(state.iid)
                    pkvalue = instance.pkvalue() or ''
                    if state.persistent:
                        spec = {'_id': pkvalue}
                        commands.append(('update', (spec, data)))
                        data = collection.update(spec, data)
                        if data['err']:
                            modified.append(Exception(data))
                        else:
                            modified.append(pkvalue)
                    else:
                        if pkvalue:
                            data['_id'] = pkvalue
                        instances.append(data)
                if instances:
                    commands.append(('insert', instances))
                    result = chain(collection.insert(instances), modified)
                    result = self.process_result(result, processed)
                else:
                    result = self.process_result(modified, processed)
                results.append((meta, result))
        return async.on_result(results, callback, commands)
    
    def process_result(self, result, iids):
        for id, iid in zip(result, iids):
            yield instance_session_result(iid, True, id, False, 0)
            
    def process_delete(self, meta, ids, result):
        if result['err']:
            yield Exception(result)
        else:
            for id in ids:
                yield instance_session_result(id, True, id, True, 0)
                
    def build(self, response, meta, fields, fields_attributes):
        if fields:
            if len(fields) == 1 and fields[0] == 'id':
                fields = ()
            else:
                fields = tuple(fields)
        else:
            fields = None
        for data in response:
            id = data.pop('_id')
            yield id, fields, data
    
    def delete_query(self, backend_query, commands):
        if backend_query is None:
            return
        query = backend_query.queryelem
        ids = ids_from_query(backend_query)
        meta = backend_query.meta
        if ids:
            for name in meta.related:
                rmanager = getattr(meta.model, name)
                rq = rmanager.query_from_query(query, ids).backend_query()
                for m, d in self.delete_query(rq, commands):
                    yield m, d
            collection = self.collection(meta)
            yield meta, self.process_delete(meta, ids,
                                            backend_query.remove(commands))
        
    def flush_structure(self, sm, commands):
        processed = False
        for instance in chain(sm._delete_query, sm.dirty):
            processed = True
            state = instance.get_state()
            binstance = instance.backend_structure()
            command = binstance.commit()
            if command:
                collection = self.collection(binstance.document._meta)
                result = collection.update(*command)
                return result
            