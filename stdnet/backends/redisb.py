import stdnet
from stdnet.utils import jsonPickler, iteritems, to_string
from stdnet import BackendDataServer, ImproperlyConfigured
from stdnet.backends.structures import structredis
from stdnet.lib import redis


class BackendDataServer(stdnet.BackendDataServer):

    structure_module = structredis
    def __init__(self, name, server, params, **kwargs):
        super(BackendDataServer,self).__init__(name,
                                               params,
                                               **kwargs)
        servs = server.split(':')
        server = servs[0]
        port   = 6379
        if len(server) == 2:
            port = int(servs[1])
        self.db              = self.params.pop('db',0)
        redispy              = redis.Redis(host = server, port = port, db = self.db)
        self.redispy         = redispy
        self.execute_command = redispy.execute_command
        self.incr            = redispy.incr
        self.clear           = redispy.flushdb
        self.sinter          = redispy.sinter
        self.sdiff           = redispy.sdiff
        self.sinterstore     = redispy.sinterstore
        self.sunionstore     = redispy.sunionstore
        self.delete          = redispy.delete
        self.keys            = redispy.keys
    
    def __repr__(self):
        r = self.redispy
        return '%s db %s on %s:%s' % (self.__name,r.db,r.host,r.port)
    
    def set_timeout(self, id, timeout):
        timeout = timeout or self.default_timeout
        if timeout:
            self.execute_command('EXPIRE', id, timeout)
    
    def has_key(self, id):
        return self.execute_command('EXISTS', id)
    
    def _set(self, id, value, timeout):
        if timeout:
            return self.execute_command('SETEX', id, timeout, value)
        else:
            return self.execute_command('SET', id, value)
    
    def _get(self, id):
        return self.execute_command('GET', id)
            
    def query(self, meta, fargs, eargs, filter_sets = None):
        '''Query a model table'''
        qset = None
        if fargs:
            filters = [meta.basekey(name,value) for name,value in fargs.iteritems()]
        else:
            filters = []
        if filter_sets:
            filters.extend(filter_sets)
        if filters:
            qset  = self.sinter(filters)
            
        if eargs:
            skeys = [meta.basekey(name,value) for name,value in fargs.iteritems()]
            eset  = self.sinter(skeys)
            if not qset:
                qset = set(hash(meta.basekey()).keys())
            return qset.difference(eset)
        else:
            if qset is None:
                return 'all'
            else:
                return qset
    
    def _set_keys(self, keys):
        items = []
        timeouts = {}
        for key,val in iteritems(keys):
            timeout = val.timeout
            if timeout:
                timeouts[key] = timeout
            items.append(key)
            items.append(val.value)
        self.execute_command('MSET', *items)
        for key,timeout in iteritems(timeouts):
            self.execute_command('EXPIRE', key, timeout)
        
    def get_object(self, meta, name, value):
        '''Retrive an object from the database. If object is not available, it raises
an :class:`stdnet.exceptions.ObjectNotFound` exception.

    * *meta* :ref:`database metaclass <database-metaclass>` or model
    * *name* name of field (must be unique)
    * *value* value of field to search.'''
        if name != 'id':
            id = self._get(meta.basekey(name,value))
        else:
            id = value
        if id is None:
            raise ObjectNotFound
        data = self.hash(meta.basekey()).get(id)
        if data is None:
            raise ObjectNotFound
        return meta.make(id,data)
    
    def save_object(self, obj, commit):
        data = []
        indexes = []
        #Loop over scalar fields first
        for field in meta.scalarfields:
            name = field.attname
            value = getattr(self,name,None)
            serializable = field.serialize(value)
            if serializable is None and field.required:
                raise FieldError("Field '{0}' has no value for model '{1}'.\
 Cannot save instance".format(field,meta))
            data.append(serializable)
            if field.index:
                indexes.append((field,serializable))
        self.id = meta.pk.serialize(self.id)
        meta.cursor.add_object(self, data, indexes, commit = commit)
        #
        meta  = obj._meta
        timeout = meta.timeout
        cache = self._cachepipe
        hash  = meta.table()
        objid = obj.id
        hash.add(objid, data)
        
        # Create indexes if possible
        for field,value in indexes:
            key     = meta.basekey(field.name,value)
            if field.unique:
                index = self.index_keys(key, timeout)
            else:
                if field.ordered:
                    index = self.ordered_set(key, timeout, pickler = nopickle)
                else:
                    index = self.unordered_set(key, timeout, pickler = nopickle)
            index.add(objid)
                
        if commit:
            self.commit()
    
    def flush(self, meta, count):
        if count is not None:
            count[str(meta)] = meta.table().size()
        keys = self.keys(meta.basekey()+b'*')
        if keys:
            self.delete(*keys)
