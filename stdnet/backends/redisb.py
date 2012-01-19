from copy import copy
import json
from hashlib import sha1

import stdnet
from stdnet import FieldValueError
from stdnet.conf import settings
from stdnet.utils import to_string, map, gen_unique_id, zip, native_str
from stdnet.lib import redis, ScriptBuilder, RedisScript, read_lua_file, \
                        pairs_to_dict
from stdnet.lib.redis import flat_mapping, Pipeline

from .base import BackendStructure, query_result, session_result

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


class build_query(RedisScript):
    script = read_lua_file('build_query.lua')
    
    
class load_query(RedisScript):
    '''Rich script for loading a query result into stdnet. It handles
loading of different fields, loading of related fields, sorting and
limiting.'''
    script = (read_lua_file('utils/table.lua'),
              read_lua_file('utils/redis.lua'),
              read_lua_file('load_query.lua'))
    
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
                 fields = None, fields_attributes = None,
                 client = None, **kwargs):
        meta = query.meta
        data, related = response
        if get:
            field = meta.dfields[get]
            tpy = field.to_python
            if get == 'id':
                return [tpy(v) for v in data]
            else:
                return [tpy(v) for _,v[1] in data]
        else:
            data = self.build(data, fields, fields_attributes)
            related_fields = {}
            if related:
                encoding = client.encoding
                for fname,rdata,fields in related:
                    fname = native_str(fname, encoding)
                    fields = tuple(native_str(f, encoding) for f in fields)
                    related_fields[fname] =\
                        self.load_related(meta, fname, rdata, fields, encoding)
            return query.backend.make_objects(meta, data, related_fields)
        
    def load_related(self, meta, fname, data, fields, encoding):
        '''Parse data for related objects.'''
        field = meta.dfields[fname]
        if field in meta.multifields:
            fmeta = field.structure_class()._meta
            if fmeta.name in ('hashtable','zset','ts'):
                return ((native_str(id, encoding),
                         pairs_to_dict(fdata, encoding)) for \
                        id,fdata in data)
            else:
                return data
        else:
            # this is data for stdmodel instances
            return self.build(data,fields,fields)
        

class delete_query(RedisScript):
    '''Lua script for bulk delete of an orm query, including cascade items.
The first parameter is the model'''
    script = read_lua_file('delete_query.lua')
    
    def callback(self, response, args, meta = None, client = None, **kwargs):
        return session_result(meta, response, 'delete')
        if response:
            meta = sm.meta
            tpy = meta.pk.to_python
            ids = []
            rem = sm.expunge
            for id in response:
                id = tpy(id)
                rem(id)
                ids.append(id)
            return ids
        else:
            return response
    

class commit_session(RedisScript):
    script = read_lua_file('session.lua')
    
    def callback(self, response, args, sm = None, client = None, **kwargs):
        return session_result(sm.meta, response[0], 'save')
        

def redis_execution(pipe, result_type):
    command = copy(pipe.command_stack)
    command.pop(0)
    result = pipe.execute()
    results = []
    for v in result:
        if isinstance(v, Exception):
            raise v
        elif isinstance(v, result_type):
            results.append(v)
    return command, results
    
    
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
        # Accumulate a query
        pipe = self.pipe
        backend = self.backend
        p = 'z' if self.meta.ordering else 's'
        meta = self.meta
        args = []
        for child in qs:
            if getattr(child,'backend',None) != backend:
                args.extend(('','' if child is None else child))
            else:
                be = child.backend_query(pipe = pipe)
                args.extend(('key',be.query_key))
                
        if qs.keyword == 'set':
            if qs.name == 'id' and not args:
                return 'key',backend.basekey(meta,'id')
            #elif len(args) == 2 and not args[0] and not qs.unique:
            #    return 'key',backend.basekey(meta,'idx', args[1])
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
                getattr(pipe,p+'interstore')(key,args)
            elif qs.keyword == 'union':
                getattr(pipe,p+'unionstore')(key,args)
            elif qs.keyword == 'diff':
                getattr(pipe,p+'diffstore')(key,args)
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
        pipe = self.pipe
        if self.meta.ordering:
            pipe.zcard(self.query_key)
        else:
            pipe.scard(self.query_key)
        pipe.add_callback(lambda r : query_result(self.query_key,r))
        self.commands, self.query_results = redis_execution(pipe, query_result)
        return self.query_results[-1].count
    
    def order(self):
        '''Perform ordering with respect model fields.'''
        if self.queryelem.ordering:
            last = self.queryelem.ordering
            desc = 'DESC' if last.desc else ''
            field = last.name
            nested = last.nested
            nested_args = []
            while nested:
                meta = nested.model._meta
                nested_args.extend((self.backend.basekey(meta),nested.name))
                last = nested
                nested = nested.nested
            meth = ''
            if last.field.internal_type == 'text':
                meth = 'ALPHA'
            
            args = [field, meth, desc, len(nested_args)//2]
            args.extend(nested_args)
            return args
            
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
        # Unwind the database query by creating a list of arguments for
        # the load_query lua script
        backend = self.backend
        meta = self.meta
        order = self.order() or ()
        start, stop = self.get_redis_slice(slic)
        get = self.queryelem._get_field
        fields_attributes = None
        args = [self.query_key, backend.basekey(meta)]
        # if the get_field is available, we simply load that field
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
        args.extend(self.related_lua_args())
        
        if order:
            if start and stop == -1:
                stop = self.execute_query()-1
            if stop != -1:
                stop = stop - start + 1
            name = 'explicit'
        else:
            name = ''
            if meta.ordering:
                name = 'DESC' if meta.ordering.desc else 'ASC'
        
        args.extend((name,start,stop))
        args.extend(order)
                    
        options = {'fields':fields,
                   'fields_attributes':fields_attributes,
                   'query':self,
                   'get':get}
        return backend.client.script_call('load_query', *args, **options)    

    def related_lua_args(self):
        '''Generator of load_related arguments'''
        related = self.queryelem.select_related
        if not related:
            yield 0
        else:
            meta = self.meta
            yield len(related)
            for rel in related:
                field = meta.dfields[rel]
                typ = 'structure' if field in meta.multifields else ''
                relmodel = field.relmodel
                bk = self.backend.basekey(relmodel._meta) if relmodel else ''
                fi = related[rel]
                yield bk
                yield field.name
                yield field.attname
                yield typ
                yield len(fi)
                for v in fi:
                    yield v
            

def iteretor_pipelined(f):
    
    def _(self):
        if self.pipelined:
            return iter(())
        else:
            return f(self)
        
    return _


class RedisStructure(BackendStructure):
    
    @iteretor_pipelined
    def __iter__(self):
        return self._iter()
        
    def _iter(self):
        raise NotImplementedError()
        
    @property
    def pipelined(self):
        return isinstance(self.client,Pipeline)
    
    
class Set(RedisStructure):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            self.client.sadd(self.id, *cache.toadd)
        if cache.toremove:
            self.client.srem(self.id, *cache.toremove)
    
    def size(self):
        return self.client.scard(self.id)
    
    def _iter(self):
        return self.client.smembers(self.id)
    

class Zset(RedisStructure):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            flat = cache.toadd.flat()
            self.client.zadd(self.id, *flat)
        if cache.toremove:
            flat = cache.toadd.flat()
            self.client.zadd(self.id, *flat)
    
    def size(self):
        return self.client.zcard(self.id)
    
    def _iter(self):
        # Redis return a value followed by score, we invert the result
        for v,s in self.client.zrange(self.id, self.start, self.stop,
                                    withscores = True):
            yield s,v
            
    def values(self, desc = False, withscores = False):
        for v in self.client.zrange(self.id, self.start, self.stop,
                                    desc = False, withscores = False):
            yield v
    

class List(RedisStructure):
    
    def flush(self):
        cache = self.instance.cache
        if cache.front:
            self.client.lpush(self.id, *cache.front)
        if cache.back:
            self.client.rpush(self.id, *cache.back)
    
    def size(self):
        return self.client.llen(self.id)
    
    def _iter(self):
        for v in self.client.lrange(self.id, self.start, self.stop):
            yield v


class Hash(RedisStructure):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            self.client.hmset(self.id, cache.toadd)
        if cache.toremove:
            self.client.hdel(self.id, *cache.toremove)
            
    def size(self):
        return self.client.hlen(self.id)
    
    def get(self, key):
        return self.client.hget(self.id, key)
    
    def __contains__(self, key):
        return self.client.hexists(self.id, key)
    
    def _iter(self):
        return iter(self.client.hkeys(self.id))
    
    @iteretor_pipelined
    def values(self):
        return iter(self.client.hvals(self.id))
    
    @iteretor_pipelined        
    def items(self):
        return iter(self.client.hgetall(self.id))
    
    
class TS(RedisStructure):
    
    def flush(self):
        cache = self.instance.cache
        if cache.toadd:
            self.client.hmset(id, cache.toadd)
        if cache.todelete:
            self.client.hdel(id, *cache.todelete)
            
    def size(self):
        return self.client.tslen(self.id)

        
struct_map = {'set':Set,
              'list':List,
              'zset':Zset,
              'hashtable':Hash,
              'ts':TS}


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
        '''Execute a session in redis.'''
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
                self.accumulate_delete(pipe, delquery)
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
                    options = {'sm': sm}
                    pipe.script_call('commit_session', *lua_data, **options)
    
        command, result = redis_execution(pipe, session_result)
        return callback(result, command)
    
    def accumulate_delete(self, pipe, backend_query):
        # Accumulate models queries for a delete. It loops through the
        # related models to build related queries.
        # We pass the pipe since the backend_query may have been evaluated
        # using a different pipe
        if backend_query is None:
            return
        query = backend_query.queryelem
        meta = query.meta
        related_queries = []
        for name in meta.related:
            rmanager = getattr(meta.model,name)
            rq = rmanager.query_from_query(query).backend_query(pipe = pipe)
            self.accumulate_delete(pipe, rq)
        s = 'z' if meta.ordering else 's'
        indices = list(self.flat_indices(meta))
        multi_fields = [field.name for field in meta.multifields]
        lua_data = [self.basekey(meta), backend_query.query_key,
                    s, len(indices)//2]
        lua_data.extend(indices)
        lua_data.append(len(multi_fields))
        lua_data.extend(multi_fields)
        options = {'meta':meta}
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
        
    def clean(self, meta):
        return self.client.delpattern(self.tempkey(meta, '*'))
            
    def model_keys(self, meta):
        pattern = '{0}*'.format(self.basekey(meta))
        return self.client.keys(pattern)            
        
    def instance_keys(self, obj):
        meta = obj._meta
        keys = [self.basekey(meta,OBJ,obj.id)]
        for field in meta.multifields:
            f = getattr(obj,field.attname)
            keys.append(f.id)
        return keys

    def structure(self, instance, session = None):
        struct = struct_map.get(instance._meta.name)
        return struct(instance,self.client)
        
    def flush_structure(self, sm, pipe = None):
        client = pipe or self.client
        struct = struct_map.get(sm.meta.name)
        for instance in sm:
            struct(instance,client).commit()
        