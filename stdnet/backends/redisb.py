from collections import namedtuple

import stdnet
from stdnet.conf import settings
from stdnet.utils import iteritems, to_string, map, gen_unique_id
from stdnet.backends.structures import structredis
from stdnet.lib import redis, connection

MIN_FLOAT =-1.e99
EMPTY_DICT = {}

################################################################################
#    prefixes for data
IDS = 'ids'
OBJ = 'obj'
UNI = 'uni'
IDX = 'idx'
TMP = 'tmp'
################################################################################

pipeattr = lambda pipe,p,name : getattr(pipe,p+name)


redis_connection = namedtuple('redis_connection',
                              'host port db password socket_timeout decode')
 

class RedisTransaction(stdnet.Transaction):
    default_name = 'redis-transaction'
    
    def _execute(self):
        '''Commit cache objects to database.'''
        cursor = self.cursor
        for id,cachepipe in iteritems(self._cachepipes):
            el = getattr(self.backend,cachepipe.method)(id)
            el._save_from_pipeline(cursor, cachepipe.pipe)
            cachepipe.pipe.clear()
                    
        if not self.emptypipe():
            return cursor.execute()
        
    def emptypipe(self):
        if hasattr(self.cursor,'execute'):
            return len(self.cursor.command_stack) <= 1
        else:
            return True


class add2set(object):

    def __init__(self, backend, pipe, meta):
        self.backend = backend
        self.pipe = pipe
        self.meta = meta
    
    def __call__(self, key, id, score = None, obj = None, idsave = True):
        ordering = self.meta.ordering
        if ordering:
            if obj is not None:
                v = getattr(obj,ordering.name,None)
                score = MIN_FLOAT if v is None else ordering.field.scorefun(v)
            elif score is None:
                # A two way trip here.
                idset = self.backend.basekey(self.meta,'id')
                score = self.backend.client.zscore(idset,id)
            if idsave:
                self.pipe.zadd(key, score, id)
        elif idsave:
            self.pipe.sadd(key, id)
        return score
        
        
class RedisQuery(stdnet.BeckendQuery):
    result = None
    _count = None
        
    def _unique_set(self, name, values):
        '''Handle filtering over unique fields'''
        meta = self.meta
        key = self.backend.tempkey(meta)
        pipe = self.pipe
        add = self.add
        if name == 'id':
            for id in values:
                add(key,id)
        else:
            bkey = self.backend.basekey
            rpy = self.backend.client
            for value in values:
                hkey = bkey(meta,UNI,name)
                id = rpy.hget(hkey, value)
                add(key,id)
        pipe.expire(key,self.expire)
        return key
    
    def _query(self, qargs, setoper, key = None, extra = None):
        pipe = self.pipe
        meta = self.meta
        backend = self.backend
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
                    keys.append(backend.basekey(meta,IDX,q.name,q.values[0]))
                else:
                    insersept = [backend.basekey(meta,IDX,q.name,value)\
                                  for value in q.values]
                    tkey = backend.tempkey(meta)
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
                key = backend.tempkey(meta)
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
        backend = self.backend
        sha = self._sha
        for q in queries:
            sha.write(q.__repr__().encode())
            query = q.query
            query._buildquery()
            qset = query.qset.query_set
            db = backend.client.db
            if db != pipe.db:
                raise ValueError('Indexes in a different database')
                # In a different redis database. We need to move the set
                query._meta.cursor.client.move(qset,pipe.db)
                pipe.expire(qset,self.expire)
            skey = self.meta.tempkey()
            okey = backend.basekey(meta,OBJ,'*->{0}'.format(q.field))
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
        backend = self.backend
        self.idset = idset = backend.basekey(meta,'id')
        p = 'z' if meta.ordering else 's'
        self.pipe = pipe = backend.client.pipeline()
        if p == 'z':
            pismember =  pipeattr(pipe,'','zrank')
            self.ismember =  pipeattr(backend.client,'','zrank')
            chk = self.zism
        else:
            pismember =  pipeattr(pipe,'','sismember')
            self.ismember =  pipeattr(backend.client,'','sismember')
            chk = self.sism
        
        if self.qs.simple:
            allids = []
            for q in fargs:
                if q.name == 'id':
                    ids = q.values
                else:
                    key = backend.basekey(meta, UNI, q.name)
                    ids = backend.client.hmget(key, q.values)
                for id in ids:
                    if id is not None:
                        allids.append(id)
                        pismember(idset,id)
            self.result = [id for (id,r) in zip(allids,pipe.execute())\
                           if chk(r)]
        else:
            self.intersect = pipeattr(pipe,p,'interstore')
            self.union = pipeattr(pipe,p,'unionstore')
            self.diff = pipeattr(pipe,p,'diffstore')
            self.card = pipeattr(backend.client,p,'card')
            self.add = add2set(backend,pipe,meta)
            if queries:
                idset = self.build_from_query(queries)
            key1 = self._query(fargs,self.intersect,idset,self.qs.filter_sets)
            key2 = self._query(eargs,self.union)
            if key2:
                key = backend.tempkey(meta)
                self.diff(key,(key1,key2)).expire(key,self.expire)
            else:
                key = key1
            self.query_set = key
            
    def execute(self):
        sha = self.sha
        if sha:
            if self.timeout:
                key = self.meta.tempkey(sha)
                if not self.backend.client.exists(key):
                    self.pipe.rename(self.query_set,key)
                    self.result = self.pipe.execute()
                self.query_set = key
            else:
                self.result = self.pipe.execute()
    
    def order(self):
        '''Perform ordering with respect model fields.'''
        meta=  self.meta
        backend = self.backend
        if self.qs.ordering:
            sort_by = self.qs.ordering
            skey = backend.tempkey(meta)
            okey = backend.basekey(meta, OBJ,'*->{0}'.format(sort_by.name))
            pipe = backend.client.pipeline()
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
        # Unwind the database query
        backend = self.backend
        client = backend.client
        
        if self.qs.simple:
            ids = self.result
            if slic:
                ids = ids[slic]
        else:
            skey = self.order()
            if skey:
                start,stop = self.get_redis_slice(slic)
                ids = self.backend.client.lrange(skey,start,stop)
            elif self.meta.ordering:
                start,stop = self.get_redis_slice(slic)
                if self.meta.ordering.desc:
                    command = client.zrevrange
                else:
                    command = client.zrange
                ids = command(self.query_set,start,stop)
            else:
                ids = list(client.smembers(self.query_set))
                if slic:
                    ids = ids[slic]
        
        # Load data
        if ids:
            meta = self.meta
            bkey = backend.basekey
            pipe = None
            fields = self.qs.fields or None
            fields_attributes = None
            if fields:
                fields, fields_attributes = meta.backend_fields(fields)
                if fields:
                    pipe = client.pipeline()
                    hmget = pipe.hmget
                    for id in ids:
                        hmget(bkey(meta, OBJ, to_string(id)),fields_attributes)
            else:
                pipe = client.pipeline()
                hgetall = pipe.hgetall
                for id in ids:
                    hgetall(bkey(meta, OBJ, to_string(id)))
            if pipe is not None:
                result = backend.make_objects(meta, ids,
                                              pipe.execute(), fields,
                                              fields_attributes)
            else:
                result = backend.make_objects(meta, ids)
            return self.load_related(result)
        else:
            return ids
    

class BackendDataServer(stdnet.BackendDataServer):
    Transaction = RedisTransaction
    Query = RedisQuery
    structure_module = structredis
    connection_pools = {}
    _redis_clients = {}
        
    def setup_connection(self, address, **params):
        self.namespace = params.get('prefix',settings.DEFAULT_KEYPREFIX)
        addr = address.split(':')
        if len(addr) == 2:
            try:
                address = (addr[0],int(addr[1]))
            except:
                pass
        cp = redis.ConnectionPool(address, **params)
        if cp in self.connection_pools:
            cp = self.connection_pools[cp]
        else:
            self.connection_pools[cp] = cp
        rpy = redis.Redis(connection_pool = cp)
        self.execute_command = rpy.execute_command
        self.clear = rpy.flushdb
        self.delete = rpy.delete
        self.keys = rpy.keys
        return rpy
    
    def cursor(self, pipelined = False):
        return self.client.pipeline() if pipelined else self.client
    
    def issame(self, other):
        return self.client == other.client
        
    def disconnect(self):
        self.client.connection_pool.disconnect()
    
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
    
    def _loadfields(self, obj, toload):
        if toload:
            fields = self.client.hmget(self.basekey(obj._meta, OBJ, obj.id), toload)
            return dict(zip(toload,fields))
        else:
            return EMPTY_DICT

    def save_object(self, obj, newid, transaction):        
        # Add object data to the model hash table
        pipe = transaction.cursor
        obid = obj.id
        meta = obj._meta
        bkey = self.basekey
        data = obj._temp['cleaned_data']
        indices = obj._temp['indices']
        if data:
            pipe.hmset(bkey(meta,OBJ,obid),data)
        #hash.addnx(objid, data)
        
        if newid or indices:
            add = add2set(self,pipe,meta)
            score = add(bkey(meta,'id'), obid, obj=obj, idsave=newid)
            fields = self._loadfields(obj,obj._temp['toload'])
            
        if indices:
            rem = pipeattr(pipe,'z' if meta.ordering else 's','rem')
            if not newid:
                pipe.delpattern(self.tempkey(meta,'*'))
            
            # Create indexes
            for field,value,oldvalue in indices:
                name = field.name
                if field.unique:
                    name = bkey(meta,UNI,name)
                    if not newid:
                        oldvalue = fields.get(field.name,oldvalue)
                        pipe.hdel(name, oldvalue)
                    pipe.hset(name, value, obid)
                else:
                    if not newid:
                        oldvalue = fields.get(field.name,oldvalue)
                        rem(bkey(meta,IDX,name,oldvalue), obid)
                    add(bkey(meta,IDX,name,value), obid, score = score)
                        
        return obj
    
    def _delete_object(self, obj, transaction):
        dbdata = obj._dbdata
        id = dbdata['id']
        # Check for multifields and remove them
        meta = obj._meta
        bkey = self.basekey
        pipe = transaction.cursor
        rem = pipeattr(pipe,'z' if meta.ordering else 's','rem')
        #remove the hash table
        pipe.delete(bkey(meta, OBJ, id))
        #remove the id from set
        rem(bkey(meta, 'id'), id)
        # Remove multifields
        mfs = obj._meta.multifields
        if mfs:
            fids = [fid for fid in (field.id(obj) for field in mfs) if fid]
            if fids:
                transaction.cursor.delete(*fids)
        # Remove indices
        if meta.indices:
            rem = pipeattr(pipe,'z' if meta.ordering else 's','rem')
            toload = []
            for field in meta.indices:
                name = field.name
                if name not in dbdata:
                    toload.append(name)
                else:
                    if field.unique:
                        pipe.hdel(bkey(meta,UNI,name), dbdata[name])
                    else:
                        rem(bkey(meta,IDX,name,dbdata[name]), id)
            fields = self._loadfields(obj,toload)
            for name,value in iteritems(fields):
                field = meta.dfields[name]
                if field.unique:
                    pipe.hdel(bkey(meta,UNI,name), value)
                else:
                    rem(bkey(meta,IDX,name,value), id)
    
    def basekey(self, meta, *args):
        """Calculate the key to access model data in the backend backend."""
        key = '{0}{1}'.format(self.namespace,meta.modelkey)
        postfix = ':'.join((str(p) for p in args if p is not None))
        return '{0}:{1}'.format(key,postfix) if postfix else key
    
    def autoid(self, meta):
        id = self.basekey(meta,IDS)
        return self.client.incr(id)
    
    def tempkey(self, meta, name = None):
        return self.basekey(meta, TMP, name if name is not None else\
                                        gen_unique_id())
        
    def flush(self, meta):
        '''Flush all model keys from the database'''
        # The scripting delete
        pattern = '{0}*'.format(self.basekey(meta))
        return self.client.delpattern(pattern)
            
    def instance_keys(self, obj):
        meta = obj._meta
        keys = [meta.basekey(meta,OBJ,obj.id)]
        for field in meta.multifields:
            f = getattr(obj,field.attname)
            keys.append(f.id)
        return keys
