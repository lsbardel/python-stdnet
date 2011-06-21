import stdnet
from stdnet.utils import jsonPickler, iteritems, to_string
from stdnet import BackendDataServer, ImproperlyConfigured
from stdnet.backends.structures import structredis
from stdnet.lib import redis

MIN_FLOAT =-1.e99

OBJ = 'obj'
UNI = 'uni'
IDX = 'idx'
setattr = lambda pipe,p,name : getattr(pipe,p+name) 

class RedisTransaction(object):
    
    def __init__(self, server, pipelined = True, cachepipes = None):
        self.server = server
        if pipelined:
            self.pipe = server.redispy.pipeline()
        else:
            self.pipe = server.redispy
        self._cachepipes = cachepipes if cachepipes is not None else {}
        
    def _get_pipe(self, id, typ, timeout):
        '''Return a pipeline object'''
        cachepipes  = self._cachepipes
        if id not in cachepipes:
            cvalue = typ(timeout)
            cachepipes[id] = cvalue
        return cachepipes[id]
    
    def commit(self):
        '''Commit cache objects to database.'''
        cachepipes = self._cachepipes
        pipe = self.pipe
        for id,cachepipe in iteritems(cachepipes):
            el = getattr(self.server,cachepipe.method)(id, transaction = self)
            el.save()        
        if hasattr(pipe,'execute'):
            return pipe.execute()
       
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()


class add2set(object):

    def __init__(self, server, pipe, meta):
        self.server = server
        self.pipe = pipe
        self.meta = meta
    
    def __call__(self, key, id, score = None, obj = None):
        ordering = self.meta.ordering
        if ordering:
            if obj is not None:
                v = getattr(obj,ordering.name,None)
                score = MIN_FLOAT if v is None else ordering.field.scorefun(v)
            elif score is None:
                # A two way trip here.
                idset = self.meta.basekey('id')
                score = self.server.redispy.zscore(idset,id)
            self.pipe.zadd(key, id, score)
        else:
            self.pipe.sadd(key, id)
        return score
        
        
class RedisQuery(object):
    result = None
    query_set = None
    sorted_list = None
    start = 0
    end = -1
    
    def __init__(self, server, meta, expire = 60):
        self.slices = {}
        self.expire = expire
        self.server = server
        self.meta = meta
        self.pipe = server.redispy.pipeline()
        p = 'z' if meta.ordering else 's'
        self.intersect = setattr(self.pipe,p,'interstore')
        self.union = setattr(self.pipe,p,'unionstore')
        self.diff = setattr(self.pipe,p,'diffstore')
        self.card = setattr(self.server.redispy,p,'card')
        self.add = add2set(server,self.pipe,meta)
        
    def _unique_set(self, name, values):
        '''Handle filtering over unique fields'''
        key = self.meta.tempkey()
        pipe = self.pipe
        add = self.add
        if name == 'id':
            for id in values:
                add(key,id)
        else:
            bkey = self.meta.basekey
            rpy = self.server.redispy
            for value in values:
                hkey = bkey(UNI,name)
                id = rpy.hget(hkey, value)
                add(key,id)
        pipe.expire(key,self.expire)
        return key
    
    def _query(self, qargs, setoper, key = None, extra = None):
        pipe = self.pipe
        meta = self.meta
        keys = []
        if qargs:
            for q in qargs:
                if q.unique:
                    keys.append(self._unique_set(q.name, q.values))
                elif len(q.values) == 1:
                    keys.append(meta.basekey(IDX,q.name,q.values[0]))
                else:
                    insersept = [meta.basekey(IDX,q.name,value) for value in q.values]
                    tkey = self.meta.tempkey()
                    self.union(tkey,insersept).expire(tkey,self.expire)
                    keys.append(tkey)
        
        if extra:
            keys.extend(extra)
        
        if keys:
            if key:
                keys.append(key)
            if len(keys) > 1:
                key = self.meta.tempkey()
                setoper(key, keys).expire(key,self.expire)
            else:
                key = keys[0]
                
        return key
    
    def querysha(self, fargs, eargs, filter_sets, sort_by):
        '''Build a unique sha for the queryset'''
        pass
        
    def __call__(self, fargs, eargs, filter_sets = None, sort_by = None):
        if self.result is not None:
            raise ValueError('Already Called')
        meta = self.meta
        pipe = self.pipe
        idset = self.meta.basekey('id')            
        #
        key1 = self._query(fargs,self.intersect,idset,filter_sets)
        key2 = self._query(eargs,self.union)
        
        if key2:
            key = self.meta.tempkey()
            self.diff(key,(key1,key2)).expire(key,self.expire)
        else:
            key = key1
        
        if pipe.command_stack:
            self.result = pipe.execute()
        else:
            self.result = 'all'
        
        if sort_by:
            skey = self.meta.tempkey()
            okey = self.meta.basekey(OBJ,'*->')+sort_by.name.encode()
            self.server.redispy.sort(key,
                                     by = okey,
                                     desc = sort_by.desc,
                                     store = skey)
        else:
            skey = None
        
        self.query_set = key
        self.sorted_list = skey
        return self
    
    def count(self):
        return self.card(self.query_set)
    
    def __len__(self):
        return self.count()
    
    def __contains__(self, val):
        return self.server.redispy.sismember(self.query_set, val)
    
    def __iter__(self):
        return iter(self.aslist())
    
    def aslist(self):
        if self.sorted_list:
            ids = self.server.redispy.lrange(self.sorted_list,\
                                             self.start,self.end)
        elif self.meta.ordering:
            if self.meta.ordering.desc:
                command = self.server.redispy.zrevrange
            else:
                command = self.server.redispy.zrange
            ids = command(self.query_set,self.start,self.end)
        else:
            ids = list(self.server.redispy.smembers(self.query_set))
            end = self.end
            if self.start and end != -1:
                if end > 0:
                    end -= 1
                ids = ids[self.start,end]
        
        bkey = self.meta.basekey
        pipe = self.server.redispy.pipeline()
        hgetall = pipe.hgetall
        for id in ids:
            hgetall(bkey(OBJ,to_string(id)))
        return list(self.server.make_objects(self.meta,ids,pipe.execute()))
    
    
class BackendDataServer(stdnet.BackendDataServer):
    Transaction = RedisTransaction
    Query = RedisQuery
    structure_module = structredis
    
    def __init__(self, name, server, params, **kwargs):
        super(BackendDataServer,self).__init__(name,
                                               params,
                                               **kwargs)
        servs = server.split(':')
        server = servs[0]
        port   = 6379
        if len(servs) == 2:
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
            
    def unwind_query(self, meta, qset):
        '''Unwind queryset'''
        table = meta.table()
        ids = list(qset)
        make_object = self.make_object
        for id,data in zip(ids,table.mget(ids)):
            yield make_object(meta,id,data)
    
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

    def _save_object(self, obj, transaction):        
        # Add object data to the model hash table
        pipe = transaction.pipe
        meta = obj._meta
        obid = obj.id
        bkey = meta.basekey
        data = obj.cleaned_data
        if data:
            pipe.hmset(bkey(OBJ,obid),data)
        #hash.addnx(objid, data)
        
        add = add2set(self,pipe,meta)
        score = add(bkey('id'), obid, obj=obj)
        
        # Create indexes
        for field,value in obj.indices:
            if field.unique:
                pipe.hset(bkey(UNI,field.name),value,obid)
            else:
                add(bkey(IDX,field.name,value), obid, score = score)
                        
        return obj
    
    def _remove_indexes(self, obj, transaction):
        obj.is_valid()
        meta = obj._meta
        obid = obj.id
        pipe = transaction.pipe
        bkey = meta.basekey
        pipe.delete(bkey(OBJ,obid))
        rem = setattr(pipe,'z' if meta.ordering else 's','rem')
        rem(bkey('id'), obid)
        
        for field,value in obj.indices:
            if field.unique:
                pipe.hdel(bkey(UNI,field.name),value)
            else:
                rem(bkey(IDX,field.name,value), obid)
    
    def _delete_object(self, obj, transaction, deleted):
        append = deleted.append
        pipe = transaction.pipe
        for field in obj._meta.multifields:
            fid = field.id(obj)
            if fid:
                pipe.delete(fid)
                append(fid)
    
    def flush(self, meta, count):
        '''Flush all model keys from the database'''
        #TODO: this should become a Lua script
        if count is not None:
            count[str(meta)] = meta.table().size()
        keys = self.keys(meta.basekey()+b'*')
        if keys:
            self.delete(*keys)
            
    def instance_keys(self, obj):
        meta = obj._meta
        keys = [meta.basekey(OBJ,obj.id)]
        for field in meta.multifields:
            f = getattr(obj,field.attname)
            keys.append(f.id)
        return keys
