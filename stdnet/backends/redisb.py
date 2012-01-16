from collections import namedtuple
from copy import copy
import json
from hashlib import sha1

import stdnet
from stdnet import FieldValueError
from stdnet.conf import settings
from stdnet.utils import to_string, map, gen_unique_id, zip
from stdnet.lib import redis, ScriptBuilder, RedisScript, read_lua_file, \
                        pairs_to_dict
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

redis_connection = namedtuple('redis_connection',
                              'host port db password socket_timeout decode')
 

class build_query(RedisScript):
    script = read_lua_file('build_query.lua')
    
    
class load_query(RedisScript):
    script = read_lua_file('load_query.lua')
    
    def build(self, response, fields, fields_attributes):
        fields = tuple(fields) if fields else None
        if fields:
            if len(fields) == 1 and fields[0] == 'id':
                for id in response:
                    yield id,(),{}
            else:
                for id,fdata in response:
                    yield id,fields_attributes,\
                            dict(zip(fields_attributes,fdata))
        else:
            for id,fdata in response:
                yield id,None,dict(pairs_to_dict(fdata))
    
    def callback(self, response, args, query = None, get = None,
                 fields = None, fields_attributes = None):
        meta = query.meta
        if get:
            field = meta.dfields[get]
            tpy = field.to_python
            if get == 'id':
                return [tpy(v) for v in response]
            else:
                return [tpy(v) for _,v[1] in response]
        else:
            data = self.build(response, fields, fields_attributes)
            result = query.backend.make_objects(meta, data)
            return query.load_related(result)
                

class delete_query(RedisScript):
    '''Lua script for bulk delete of an orm query, including cascade items.
The first parameter is the model'''
    script = read_lua_file('delete_query.lua')
    
    def callback(self, response, args, session = None, meta = None):
        if response:
            tpy = meta.pk.to_python
            ids = []
            sm = session.model(meta)
            rem = sm.expunge if sm is not None else lambda x : x
            for id in response:
                id = tpy(id)
                rem(id)
                ids.append(id)
            return ids
        else:
            return response
    

class commit_session(RedisScript):
    script = read_lua_file('session.lua')
    
    def callback(self, response, args, session = None, callback = None):
        # The session has received the callback from redis client
        if callback:
            callback(response)
        return session
        

def redis_execution(pipe):
    command = copy(pipe.command_stack)
    command.pop(0)
    result = pipe.execute()
    for v in result:
        if isinstance(v,Exception):
            raise v
    return command,result
    
    
################################################################################
##    REDIS QUERY CLASS
################################################################################
class RedisQuery(stdnet.BackendQuery):
        
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
    
    def accumulate(self, qs):
        pipe = self.pipe
        backend = self.backend
        if getattr(qs,'backend',None) != backend:
            return ('',qs)
        p = 'z' if self.meta.ordering else 's'
        meta = self.meta
        if qs.keyword == 'query':
            bq = qs.backend_query(pipe = pipe)
            args = ('key',bq.query_key)
        else:
            args = []
            for child in qs:
                arg = self.accumulate(child)
                args.extend(arg)
        if qs.keyword in ('set','query'):
            if qs.name == 'id' and not args:
                return 'key',backend.basekey(meta,'id')
            else:
                bk = backend.basekey(meta)
                key = backend.tempkey(meta)
                unique = 'u' if qs.unique else ''
                pipe.script_call('build_query', bk, p, key, qs.name,
                                 unique, qs.lookup, *args)
                return 'key',key
        # a select operation
        else:
            key = backend.tempkey(meta)
            args = args[1::2]
            if qs.keyword == 'intersect':
                getattr(pipe,p+'interstore')(key,*args)
            elif qs.keyword == 'union':
                getattr(pipe,p+'unionstore')(key,*args)
            elif qs.keyword == 'diff':
                getattr(pipe,p+'diffstore')(key,*args)
            else:
                raise ValueError('Could not perform "{0}" operation'\
                                 .format(qs.keyword))
            return 'key',key
        
    def _build(self, pipe = None, **kwargs):
        '''Set up the query for redis'''
        backend = self.backend
        client = backend.client
        self.pipe = pipe if pipe is not None else client.pipeline()
        what, key = self.accumulate(self.queryelem)
        if what == 'key':
            self.query_key = key
        else:
            raise valueError('Critical error while building query')
        if self.meta.ordering:
            self.ismember = getattr(client,'zrank')
            self.card = getattr(client,'scard')
            self._check_member = self.zism
        else:
            self.ismember = getattr(client,'sismember')
            self.card = getattr(client,'zcard')
            self._check_member = self.sism
    
    def _execute_query(self):
        '''Execute the query without fetching data. Returns the number of
elements in the query.'''
        if self.meta.ordering:
            self.pipe.zcard(self.query_key)
        else:
            self.pipe.scard(self.query_key)
        self.executed_command, self.query_results = redis_execution(self.pipe)
        return self.query_results[-1]
        
    def order(self, start, stop):
        '''Perform ordering with respect model fields.'''
        if self.queryelem.ordering:
            sort_by = self.queryelem.ordering
            skey = self.backend.tempkey(self.meta)
            okey = backend.basekey(meta, OBJ,'*->{0}'.format(sort_by.name))
            order = ['BY', okey, 'LIMIT', start, stop]
            if sort_by.field.internal_type == 'text':
                order.append('ALPHA')
            return order
    
    def _has(self, val):
        r = self.ismember(self.query_key, val)
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
        get = self.queryelem._get_field
        fields_attributes = None
        args = [self.query_key, backend.basekey(meta)]
        if get:
            if get == 'id':
                fields_attributes = fields = (get,)
            else:
                fields, fields_attributes = meta.backend_fields((get,))
        else:
            fields = self.queryelem.fields or None
            if fields == ('id',):
                fields_attributes = fields
            elif fields:
                fields, fields_attributes = meta.backend_fields(fields)
            else:
                fields_attributes = ()
                
        args.append(len(fields_attributes))
        args.extend(fields_attributes)
        
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
                    
        options = {'fields':fields,
                   'fields_attributes':fields_attributes,
                   'query':self,
                   'get':get}
        return backend.client.script_call('load_query', *args, **options)    


class Struct(object):
    __slots__ = ('instance','client')
    def __init__(self, instance, client):
        self.instance = instance
        self.client = client
        if not instance.id:
            instance.id = instance.makeid()
            
    def commit(self):
        self.flush()
        self.clear()
    
    @property
    def id(self):
        return self.instance.id
        

class Set(Struct):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            self.client.sadd(self.id, *cache.toadd)
        if cache.toremove:
            self.client.srem(self.id, *cache.toremove)
    
    def size(self):
        return self.client.scard(self.id)
    
    def clear(self):
        self.instance.cache.toadd.clear()
        self.instance.cache.toremove.clear()
    

class Zset(Struct):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            self.client.sadd(self.id, *cache.toadd)
        if cache.toremove:
            self.client.sadd(self.id, *cache.toremove)
    
    def size(self):
        return self.client.scard(self.id)
    

class List(Struct):
    
    def flush(self):
        cache = self.instance.cache
        if cache.front:
            self.client.lpush(id, *cache.front)
        if cache.back:
            self.client.rpush(id, *cache.back)
    
    def size(self):
        return self.client.llen(self.id)
    

class Hash(Set):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            self.client.hmset(self.id, cache.toadd)
        if cache.toremove:
            self.client.hdel(self.id, *cache.toremove)
            
    def size(self):
        return self.client.hlen(self.id)
    
    
class TS(Struct):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            self.client.hmset(id, cache.toadd)
        if cache.todelete:
            self.client.hdel(id, *cache.todelete)
            
    def size(self):
        return self.client.hlen(self.id)
        
struct_map = {'set':Set,'list':List,'zset':Zset,'hash':Hash,'ts':TS}


################################################################################
##    REDIS BACKEND
################################################################################
class BackendDataServer(stdnet.BackendDataServer):
    Query = RedisQuery
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
            fields = self.client.hmget(self.basekey(obj._meta, OBJ, obj.id),
                                       toload)
            return dict(zip(toload,fields))
        else:
            return EMPTY_DICT
    
    def flat_indices(self, meta):
        for idx in meta.indices:
            yield idx.attname
        for idx in meta.indices:
            yield 1 if idx.unique else 0
        
    def execute_session(self, session, callback):
        basekey = self.basekey
        lua_data = []
        pipe = self.client.pipeline()
        for sm in session:
            sm.pre_commit()
            meta = sm.meta
            model_type = meta.model._model_type
            if model_type == 'structure':
                self.flush_structure(sm, pipe)
            elif model_type == 'object':
                delquery = sm.get_delete_query(pipe = pipe)
                self.accumulate_delete(delquery)
                N = len(sm)
                if N:
                    bk = basekey(meta)
                    s = 'z' if meta.ordering else 's'
                    indices = list(self.flat_indices(meta))
                    lua_data.extend((bk,s,N,len(indices)//2))
                    lua_data.extend(indices)
                    for instance in sm:
                        state = instance.state()
                        if not instance.is_valid():
                            raise FieldValueError(
                                        json.dumps(instance._dbdata['errors']))
                        score = MIN_FLOAT
                        if meta.ordering:
                            v = getattr(instance,meta.ordering.name,None)
                            if v is not None:
                                score = meta.ordering.field.scorefun(v)
                        data = instance._dbdata['cleaned_data']
                        id = instance.id or ''
                        data = flat_mapping(data)
                        action = 'c' if state.persistent else 'a'
                        lua_data.extend((action,id,score,len(data)))
                        lua_data.extend(data)
        if lua_data:
            options = {'session': session, 'callback':callback}
            pipe.script_call('commit_session', *lua_data, **options)
    
        transaction = session.transaction
        command, result = redis_execution(pipe)
        transaction.command = command
        transaction.result = result
        return transaction
    
    def accumulate_delete(self, query):
        # Accumulate models queries for a delete. It loops through the
        # related models to build related queries.
        if query is None:
            return
        meta = query.meta
        pipe = query.pipe
        related_queries = []
        for name in meta.related:
            rmanager = getattr(meta.model,name)
            rq = rmanager.query_from_query(query.queryelem)\
                         .backend_query(pipe = pipe)
            self.accumulate_delete(rq)
        s = 'z' if meta.ordering else 's'
        indices = list(self.flat_indices(meta))
        mfields = [field.name for field in meta.multifields]
        lua_data = [self.basekey(meta), query.query_key, s, len(indices)//2]
        lua_data.extend(indices)
        lua_data.append(len(mfields))
        lua_data.extend(mfields)
        options = {'meta':meta, 'session': query.session}
        pipe.script_call('delete_query', *lua_data, **options)
        return query
        
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
            
    def model_keys(self, model):
        pattern = '{0}*'.format(self.basekey(model._meta))
        return self.client.keys(pattern)            
        
    def instance_keys(self, obj):
        meta = obj._meta
        keys = [self.basekey(meta,OBJ,obj.id)]
        for field in meta.multifields:
            f = getattr(obj,field.attname)
            keys.append(f.id)
        return keys

    def flush_structure(self, sm, pipe):
        client = client or self.client
        struct = struct_map.get(sm.meta.name)
        for instance in sm:
            struct(instance,client).commit()
        