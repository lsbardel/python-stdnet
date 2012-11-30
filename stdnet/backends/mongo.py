try:
    import pymongo
except ImportError:
    raise ImportError('Mongo backend requires pymongo')

import stdnet
from stdnet.utils import unique_tuple

################################################################################
##    MONGODB QUERY CLASS
################################################################################
class MongoDbQuery(stdnet.BackendQuery):
    
    def _build(self, pipe=None, **kwargs):
        collection = self.backend.collection(self.meta)
        self.query = collection.find()
        self.pipe = pipe = pipe if pipe is not None else []
        qs = self.queryelem
        kwargs = {}
        pkname = self.meta.pkname()
        for child in qs:
            if getattr(child, 'backend', None) == self.backend:
                lookup, value = 'set', child
            else:
                lookup, value = child
            if lookup == 'set':
                be = value.backend_query(pipe=pipe)
                keys.append(be.query_key)
                args.extend(('set', be.query_key))
            elif lookup == 'value':
                kwargs[qs.name] = value
        #
        if qs.keyword == 'set':
            query = collection.find(**kwargs)
        else:
            if qs.keyword == 'intersect':
                command = getattr(pipe, p+'interstore')
            elif qs.keyword == 'union':
                command = getattr(pipe, p+'unionstore')
            elif qs.keyword == 'diff':
                command = getattr(pipe, p+'diffstore')
            else:
                raise ValueError('Could not perform %s operation' % qs.keyword)
            command(key, keys, script_dependency='move2set')
        self.query = query
    
    def _execute_query(self):
        return self.query.count()
        
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
            else:
                fields_attributes = ()
        if fields_attributes:
            pass
        return self.backend.build_query(meta, fields, self.query)
            
    
    
################################################################################
##    MONGODB BACKEND
################################################################################
class BackendDataServer(stdnet.BackendDataServer):
    Query = MongoDbQuery
    _redis_clients = {}
        
    def setup_connection(self, address):
        addr = address.split(':')
        if len(addr) == 2:
            port = int(addr[1])
        else:
            port = 27017
        db = self.params.pop('db', None)
        if not db:
            db = self.namespace.replace('.','')
        self.namespace = ''
        if not db:
            db = 'test'
        mdb = pymongo.MongoClient(addr[0], port, **self.params)
        self.params['db'] = db
        return mdb
    
    @property
    def db(self):
        return getattr(self.client, self.params['db'])
    
    def collection(self, meta):
        return getattr(self.db, self.basekey(meta))
    
    def flush(self, meta=None):
        '''Flush all model keys from the for a pattern'''
        if meta is not None:
            return self.db.drop_collection(self.basekey(meta))
        else:
            return self.client.drop_database(self.params['db'])
        
    def execute_session(self, session, callback):
        '''Execute a session in mongo.'''
        for sm in session:
            meta = sm.meta
            model_type = meta.model._model_type
            if model_type == 'structure':
                self.flush_structure(sm, pipe)
            elif model_type == 'object':
                instances = []
                processed = []
                for instance in sm.iterdirty():
                    if not instance.is_valid():
                        raise FieldValueError(
                                    json.dumps(instance._dbdata['errors']))
                    state = instance.state()
                    data = instance._dbdata['cleaned_data']
                    instances.append(data)
                    processed.append(state.iid)
                self.collection(meta).insert(instances)