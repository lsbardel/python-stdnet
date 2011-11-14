from collections import namedtuple

import stdnet
from stdnet.utils import iteritems, to_string, map
from stdnet import BackendDataServer, ImproperlyConfigured, BeckendQuery
from stdnet.backends.structures import structredis
from stdnet.lib import redis, connection

MIN_FLOAT =-1.e99

OBJ = 'obj'
UNI = 'uni'
IDX = 'idx'
setattr = lambda pipe,p,name : getattr(pipe,p+name)


redis_connection = namedtuple('redis_connection',
                              'host port db password socket_timeout decode')
 

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
        
        
class RedisQuery(BeckendQuery):
    result = None
    _count = None
        
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
        sha  = self._sha
        if qargs:
            for q in qargs:
                sha.write(q.__repr__().encode())
                if q.unique:
                    if q.lookup == 'in':
                        keys.append(self._unique_set(q.name, q.values))
                    else:
                        raise ValueError('Not available')
                elif len(q.values) == 1:
                    keys.append(meta.basekey(IDX,q.name,q.values[0]))
                else:
                    insersept = [meta.basekey(IDX,q.name,value)\
                                  for value in q.values]
                    tkey = self.meta.tempkey()
                    if q.lookup == 'in':
                        self.union(tkey,insersept).expire(tkey,self.expire)
                    #elif q.lookup == 'contains':
                    #    self.intersect(tkey,insersept).expire(tkey,self.expire)
                    else:
                        raise ValueError('Lookup {0} Not available'\
                                         .format(q.lookup))    
                    keys.append(tkey)
                    
        
        if extra:
            for id in extra:
                sha.write(id.encode('utf-8'))
                keys.append(id)
        
        if keys:
            if key:
                keys.append(key)
            if len(keys) > 1:
                key = self.meta.tempkey()
                setoper(key, keys).expire(key,self.expire)
            else:
                key = keys[0]
                
        return key
        
    def zism(self, r):
        return r is not None
    
    def sism(self, r):
        return r
    
    def build_from_query(self, queries):
        '''Build a set of ids from an external query (a query on a
different model) which has a *field* containing current model ids.'''
        keys = []
        pipe = self.pipe
        sha = self._sha
        for q in queries:
            sha.write(q.__repr__().encode())
            query = q.query 
            query._buildquery()
            qset = query.qset.query_set
            db = query._meta.cursor.redispy.db
            if db != pipe.db:
                raise ValueError('Indexes in a different database')
                # In a different redis database. We need to move the set
                query._meta.cursor.redispy.move(qset,pipe.db)
                pipe.expire(qset,self.expire)
            skey = self.meta.tempkey()
            okey = query._meta.basekey(OBJ,'*->{0}'.format(q.field))
            pipe.sort(qset, by = 'nosort', get = okey, storeset = skey)\
                    .expire(skey,self.expire)
            keys.append(skey)
        if len(keys) == 1:
            tkey = keys[0]
        else:
            tkey = self.meta.tempkey()
            self.intersect(tkey,keys).expire(tkey,self.expire)
        return tkey
    
    def build(self, fargs, eargs, queries):
        meta = self.meta
        server = self.server
        self.idset = idset = meta.basekey('id')
        p = 'z' if meta.ordering else 's'
        self.pipe = pipe = server.redispy.pipeline()
        if p == 'z':
            pismember =  setattr(pipe,'','zrank')
            self.ismember =  setattr(server.redispy,'','zrank')
            chk = self.zism
        else:
            pismember =  setattr(pipe,'','sismember')
            self.ismember =  setattr(server.redispy,'','sismember')
            chk = self.sism
        
        if self.qs.simple:
            allids = []
            for q in fargs:
                if q.name == 'id':
                    ids = q.values
                else:
                    key = meta.basekey(UNI,q.name)
                    ids = server.redispy.hmget(key, q.values)
                for id in ids:
                    if id is not None:
                        allids.append(id)
                        pismember(idset,id)
            self.result = [id for (id,r) in zip(allids,pipe.execute())\
                           if chk(r)]
        else:
            self.intersect = setattr(pipe,p,'interstore')
            self.union = setattr(pipe,p,'unionstore')
            self.diff = setattr(pipe,p,'diffstore')
            self.card = setattr(server.redispy,p,'card')
            self.add = add2set(server,pipe,meta)
            if queries:
                idset = self.build_from_query(queries)
            key1 = self._query(fargs,self.intersect,idset,self.qs.filter_sets)
            key2 = self._query(eargs,self.union)
            if key2:
                key = meta.tempkey()
                self.diff(key,(key1,key2)).expire(key,self.expire)
            else:
                key = key1
            self.query_set = key
            
    def execute(self):
        sha = self.sha
        if sha:
            if self.timeout:
                key = self.meta.tempkey(sha)
                if not self.server.redispy.exists(key):
                    self.pipe.rename(self.query_set,key)
                    self.result = self.pipe.execute()
                self.query_set = key
            else:
                self.result = self.pipe.execute()
    
    def order(self):
        '''Perform ordering with respect model fields.'''
        if self.qs.ordering:
            sort_by = self.qs.ordering
            skey = self.meta.tempkey()
            okey = self.meta.basekey(OBJ,'*->{0}'.format(sort_by.name))
            pipe = self.server.redispy.pipeline()
            pipe.sort(self.query_set,
                      by = okey,
                      desc = sort_by.desc,
                      store = skey,
                      alpha = sort_by.field.internal_type == 'text')\
                .expire(skey,self.expire).execute()
            return skey
    
    def count(self):
        if self._count is None:
            if self.qs.simple:
                self._count = len(self.result)
            else:
                self._count = self.card(self.query_set)
        return self._count
    
    def has(self, val):
        if self.qs.simple:
            return val in self.result
        else:
            return True if self.ismember(self.query_set, val) else False
    
    def get_redis_slice(self, slic):
        if slic:
            start = slic.start or 0
            stop = slic.stop or -1
            if stop > 0:
                stop -= 1
        else:
            start = 0
            stop = -1
        return start,stop
    
    def items(self, slic):
        if self.qs.simple:
            ids = self.result
            if slic:
                ids = ids[slic]
        else:
            skey = self.order()
            if skey:
                start,stop = self.get_redis_slice(slic)
                ids = self.server.redispy.lrange(skey,start,stop)
            elif self.meta.ordering:
                start,stop = self.get_redis_slice(slic)
                if self.meta.ordering.desc:
                    command = self.server.redispy.zrevrange
                else:
                    command = self.server.redispy.zrange
                ids = command(self.query_set,start,stop)
            else:
                ids = list(self.server.redispy.smembers(self.query_set))
                if slic:
                    ids = ids[slic]
        
        if ids:
            bkey = self.meta.basekey
            pipe = self.server.redispy.pipeline()
            hgetall = pipe.hgetall
            for id in ids:
                hgetall(bkey(OBJ,to_string(id)))
            return list(self.server.make_objects(self.meta,ids,pipe.execute()))
        else:
            return ids
    

class BackendDataServer(stdnet.BackendDataServer):
    Transaction = RedisTransaction
    Query = RedisQuery
    structure_module = structredis
    connection_pools = {}
    _redis_clients = {}
    
    def __init__(self, name, server, db = 0,
                 password = None, socket_timeout = None,
                 decode = None, **params):
        super(BackendDataServer,self).__init__(name,**params)
        servs = server.split(':')
        host = servs[0] if servs[0] is not 'localhost' else '127.0.0.1'
        port = int(servs[1]) if len(servs) == 2 else 6379
        socket_timeout = int(socket_timeout) if socket_timeout else None
        cp = redis_connection(host, port, db, password, socket_timeout, decode)
        if cp in self.connection_pools:
            connection_pool = self.connection_pools[cp]
        else:
            connection_pool = redis.ConnectionPool(**cp._asdict())
            self.connection_pools[cp] = connection_pool 
        redispy = redis.Redis(connection_pool = connection_pool)
        self.redispy = redispy
        self.execute_command = redispy.execute_command
        self.incr            = redispy.incr
        self.clear           = redispy.flushdb
        self.delete          = redispy.delete
        self.keys            = redispy.keys
    
    def disconnect(self):
        self.redispy.connection_pool.disconnect()
    
    def unwind_query(self, meta, qset):
        '''Unwind queryset'''
        table = meta.table()
        ids = list(qset)
        make_object = self.make_object
        for id,data in zip(ids,table.mget(ids)):
            yield make_object(meta,id,data)
    
    def set_timeout(self, id, timeout):
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
        rem = setattr(pipe,'z' if meta.ordering else 's','rem')
        keys = self.redispy.keys(meta.tempkey('*'))
        #remove the hash table and temp keys
        pipe.delete(bkey(OBJ,obid),*keys)
        #remove the id from set
        rem(bkey('id'), obid)
            
        if obj.indices:            
            for field,value in obj.indices:
                if field.unique:
                    pipe.hdel(bkey(UNI,field.name),value)
                else:
                    rem(bkey(IDX,field.name,value), obid)
    
    def _delete_object(self, obj, transaction):
        fids = obj._meta.multifields_ids_todelete(obj)
        if fids:
            transaction.pipe.delete(*fids)
    
    def flush(self, meta, count):
        '''Flush all model keys from the database'''
        #TODO: this should become a Lua script
        if count is not None:
            count[str(meta)] = meta.table().size()
        keys = self.keys('{0}*'.format(meta.basekey()))
        if keys:
            self.delete(*keys)
            
    def instance_keys(self, obj):
        meta = obj._meta
        keys = [meta.basekey(OBJ,obj.id)]
        for field in meta.multifields:
            f = getattr(obj,field.attname)
            keys.append(f.id)
        return keys
