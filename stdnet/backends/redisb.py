from collections import namedtuple
import json

import stdnet
from stdnet.conf import settings
from stdnet.utils import iteritems, to_string, map, gen_unique_id
from stdnet.backends.structures import structredis
from stdnet.lib import redis, ScriptBuilder, RedisScript
from stdnet.lib.redis import flat_mapping

MIN_FLOAT =-1.e99
EMPTY_DICT = {}

################################################################################
#    prefixes for data
ID = 'id'       # the set of all ids
OBJ = 'obj'     # the hash table for a instance
UNI = 'uni'     # the hashtable for the unique field value to id mapping
IDX = 'idx'     # the set of indexes for a field value
TMP = 'tmp'     # temorary key
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


def whereselect(oper, args):
    if args:
        yield oper
        yield len(args)
        for arg in args:
            for a in arg:
                yield a
                
                
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
    
    def _query(self, qargs, oper, key = None, extra = None):
        pipe = self.pipe
        meta = self.meta
        backend = self.backend
        keys = []
        sha  = self._sha
        if qargs:
            for q in qargs:
                sha.write(q.__repr__().encode())
                values = q.values
                if q.unique:
                    if q.lookup == 'in':
                        keys.append(self._unique_set(q.name, values))
                    else:
                        raise ValueError('Not available')
                elif len(q.values) == 1:
                    keys.append(backend.basekey(meta,IDX,q.name,q.values[0]))
                else:
                    insersept = [backend.basekey(meta,IDX,q.name,value)\
                                  for value in q.values]
                    tkey = backend.tempkey(meta)
                    if q.lookup == 'in':
                        self.union(tkey,*insersept).expire(tkey,self.expire)
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
                setoper(key, *keys).expire(key, self.expire)
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
    
    def _build(self, fargs, eargs, queries):
        '''Set up the query for redis'''
        meta = self.meta
        backend = self.backend
        idset = backend.basekey(meta, ID)
        if meta.ordering:
            p = 'z'
            self.client_ismember =  pipeattr(backend,client,'','zrank')
            self._check_member = self.zism
        else:
            p = 's'
            self.client_ismember =  pipeattr(backend.client,'','sismember')
            self._check_member = self.sism
            
        self.args = args = [backend.basekey(meta),p]
        args.extend(whereselect('intersect',fargs))
        args.extend(whereselect('union ',fargs))
        args.extend(union(fargs))
            for q in fargs:
                args.append('where')
                args.extend(q)
        if eargs:
            for q in eargs:
                args.append('exclude')
                args.extend(q)
        return
        
        if self.qs.simple:
            self.simple = True
            key = backend.tempkey(meta)
            args.append(key)
            for q in fargs:
                args.append(q.name)
                args.append(len(q.values))
                args.extend(q.values)
        else:
            # build the lua script which will execute the query
            self.simple = False
            self.client_card = getattr(backend.client, p+'card')
            #if queries:
            #    idset = self.build_from_query(queries)
            
            key1 = self._query(fargs,'intersect', idset, self.qs.filter_sets)
            key2 = self._query(eargs, 'union')
            if key2:
                key = backend.tempkey(meta)
                self.diff(key,key1,key2).expire(key,self.expire)
            else:
                key = key1
        self.query_key = key
    
    def _execute_query(self):
        '''Execute the query without fetching data. Returns the number of
elements in the query.'''
        if self.simple:
            return self.backend.client.script_call('simple_query',*self.args)
        else:
            if self.sha and self.timeout:
                key = self.meta.tempkey(sha)
                if not self.backend.client.exists(key):
                    self.pipe.rename(self.query_key,key)
                    self.query_key = key
                    self.card(self.query_key)
                    return self.pipe.execute()
                else:
                    self.query_key = key
                    return self.client_card(self.query_key)
            else:
                self.card(self.query_key)
                r = self.pipe.execute()
                self.query_string = self.pipe.script
                return r
        
    def order(self, start, stop):
        '''Perform ordering with respect model fields.'''
        if self.qs.ordering:
            sort_by = self.qs.ordering
            skey = self.backend.tempkey(self.meta)
            okey = backend.basekey(meta, OBJ,'*->{0}'.format(sort_by.name))
            order = ['BY', okey, 'LIMIT', start, stop]
            if sort_by.field.internal_type == 'text':
                order.append('ALPHA')
            return order
    
    def _has(self, val):
        r = self.client_ismember(self.query_key, val)
        return self._check_member(r)
    
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
    
    def _items(self, slic):
        # Unwind the database query
        backend = self.backend
        meta = self.meta
        start, stop = self.get_redis_slice(slic)
        order = self.order(start, stop)
        fields = self.qs.fields or None
        if fields:
            fields, fields_attributes = meta.backend_fields(fields)
        else:
            fields_attributes = ''
            
        args = [self.query_key,
                backend.basekey(meta),
                fields_attributes]
        
        if order:
            args.append('explicit')
            args.extend(order)
        else:
            if meta.ordering:
                order = 'DESC' if meta.ordering.desc else 'ASC'
            else:
                order = ''
            args.append(order)
            args.append(start)
            args.append(stop)
                    
        options = {'fields':fields,'fields_attributes':fields_attributes}
        data = backend.client.script_call('load_query', *args, **options)
        result = backend.make_objects(meta, data)
        return self.load_related(result)
    
        if fields:
                pipe = client.pipeline()
                hmget = pipe.hmget
                for id in ids:
                    hmget(bkey(meta, OBJ, to_string(id)),fields_attributes)
        
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
    

class DeleteQuery(RedisScript):
    '''Lua script for bulk delete of an orm query, including cascade items.
The first parameter is the model'''
    script = '''\
        
'''

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
        
    def execute_session(self, session):
        basekey = self.basekey
        lua_data = []
        for state in session:
            meta = state.meta
            bk = basekey(meta)
            s = 'z' if meta.ordering else 's'
            indices = tuple((idx.name for idx in meta.indices))
            uniques = tuple((1 if idx.unique else 0 for idx in meta.indices))
            if state.deleted:
                fids = meta.multifields_ids_todelete(state.instance)
                lua_data.extend(('del',bk,state.id,s,len(fids)))
                lua_data.extend(fids)
            else:
                data = state.cleaned_data()
                id = data.pop('id','')
                data = flat_mapping(data)
                action = 'change' if state.persistent else 'add'
                lua_data.extend((action,bk,id,s,len(data)))
                lua_data.extend(data)
            lua_data.append(len(indices))
            lua_data.extend(indices)
            lua_data.extend(uniques)
        options = {'session': session}
        return self.client.script_call('commit_session', *lua_data, **options)

    def basekey(self, meta, *args):
        """Calculate the key to access model data in the backend backend."""
        key = '{0}{1}'.format(self.namespace,meta.modelkey)
        postfix = ':'.join((str(p) for p in args if p is not None))
        return '{0}:{1}'.format(key,postfix) if postfix else key
    
    def tempkey(self, meta, name = None):
        return self.basekey(meta, TMP, name if name is not None else\
                                        gen_unique_id())
        
    def flush(self, meta = None, pattern = None):
        '''Flush all model keys from the database'''
        if meta is not None:
            pattern = '{0}*'.format(self.basekey(meta))
        if pattern:
            return self.client.delpattern(pattern)
            
    def instance_keys(self, obj):
        meta = obj._meta
        keys = [self.basekey(meta,OBJ,obj.id)]
        for field in meta.multifields:
            f = getattr(obj,field.attname)
            keys.append(f.id)
        return keys
