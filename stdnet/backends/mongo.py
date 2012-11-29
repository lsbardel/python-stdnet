import stdnet

try:
    import pymongo
except ImportError:
    raise ImportError('Mongo backend requires pymongo')

################################################################################
##    MONGODB QUERY CLASS
################################################################################
class MongoDbQuery(stdnet.BackendQuery):
    
    def _build(self, pipe=None, **kwargs):
        collection = self.backend.collection(self.meta)
        self.query = collection.find()
        return
        self.pipe = pipe = pipe if pipe is not None else []
        qs = self.queryelem
        pkname = meta.pkname()
        for child in qs:
            if getattr(child, 'backend', None) == backend:
                lookup, value = 'set', child
            else:
                lookup, value = child
            if lookup == 'set':
                be = value.backend_query(pipe=pipe)
                keys.append(be.query_key)
                args.extend(('set', be.query_key))
            else:
                args.extend((lookup, '' if value is None else value))
        #
        if qs.keyword == 'set':
            if qs.name == pkname and not args:
                key = backend.basekey(meta, 'id')
                temp_key = False
            else:
                key = backend.tempkey(meta)
                keys.insert(0, key)
                backend.odmrun(pipe, 'query', meta, keys, self.meta_info,
                               qs.name, *args)
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
        # If we are getting a field (for a subsequent query maybe)
        # unwind the query and store the result
        gf = qs._get_field 
        if gf and gf != pkname:
            field_attribute = meta.dfields[gf].attname
            bkey = key
            if not temp_key:
                temp_key = True
                key = backend.tempkey(meta)
            okey = backend.basekey(meta, OBJ, '*->' + field_attribute)
            pipe.sort(bkey, by='nosort', get=okey, store=key)
            self.card = getattr(pipe, 'llen')
        if temp_key:
            pipe.expire(key, self.expire)
        self.query = key
    
    def _execute_query(self):
        return self.query.count()
        
        
    
    
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