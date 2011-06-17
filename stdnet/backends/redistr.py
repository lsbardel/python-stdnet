import json

from stdnet.utils import zip, iteritems, to_bytestring
from stdnet.exceptions import FieldError, ObjectNotFound

from .base import nopickle
from .redisb import BackendDataServer as BackendDataServer0


class ordering_pickle:
    
    @classmethod
    def loads(cls,x):
        return x
    
    @classmethod
    def dumps(cls, x):
        return x.id
    

class Transaction(object):
    
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
            pipe.execute()
       
    def __enter__(self):
        return self
     
    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()
        
        
class RedisQuery(object):
    result = None
    query_set = None
    sorted_list = None
    start = 0
    end = -1
    
    def __init__(self, server, meta, expire = 20):
        self.slices = {}
        self.expire = expire
        self.server = server
        self.meta = meta
        self.pipe = server.redispy.pipeline()
    
    def _unique_set(self, name, values):
        '''Handle filtering over unique fields'''
        key = self.meta.tempkey()
        pipe = self.pipe
        if name == 'id':
            for id in values:
                pipe.sadd(key,id)
        else:
            for value in values:
                hkey = self.meta.basekey(name)
                id = self.server.hash(hkey, pickler = nopickle).get(value)
                pipe.sadd(key,id)
        pipe.expire(key,self.expire)
        return key
    
    def _query(self, kwargs, setoper, key = None, extra = None):
        pipe = self.pipe
        meta = self.meta
        keys = []
        if kwargs:
            for name,data in iteritems(kwargs):
                values,unique = data
                if unique:
                    keys.append(self._unique_set(name, values))
                elif values:
                    if len(values) == 1:
                        keys.append(meta.basekey(name,values[0]))
                    else:
                        insersept = [meta.basekey(name,value) for value in values]
                        tkey = self.meta.tempkey()
                        pipe.sunionstore(tkey,insersept).expire(tkey,self.expire)
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
    
    def __call__(self, fargs, eargs, filter_sets = None, order_by = None):
        if self.result is not None:
            raise ValueError('Already Called')
        
        pipe = self.pipe
        idset = self.meta.basekey('id')
        key1 = self._query(fargs,pipe.sinterstore,idset,filter_sets)
        key2 = self._query(eargs,pipe.sunionstore)
        
        if key2:
            key = self.meta.tempkey()
            pipe.sdiffstore(key,(key1,key2)).expire(key,self.expire)
        else:
            key = key1
        
        self.result = pipe.execute()
        
        if order_by:
            desc = False
            if order_by.startswith('-'):
                order_by = order_by[1:]
                desc = True
            skey = self.meta.tempkey()
            okey = self.meta.basekey(order_by,'id')+b'->*'
            self.server.redispy.sort(key, by = okey,
                                     desc = desc, store = skey)
        else:
            skey = None
        
        self.query_set = key
        self.sorted_list = skey
        return self
    
    def count(self):
        return self.server.redispy.scard(self.query_set)
    
    def __len__(self):
        return self.count()
    
    def __contains__(self, val):
        return self.server.redispy.sismember(self.query_set, val)
    
    def __iter__(self):
        return iter(self.aslist())
    
    def aslist(self):
        if self.sorted_list:
            res = self.server.redispy.lrange(self.sorted_list,\
                                             self.start,self.end)
        else:
            res = list(self.server.redispy.smembers(self.query_set))
            end = self.end
            if self.start and end != -1:
                if end > 0:
                    end -= 1
                res = res[self.start,end]
        
        res = list(self.server.unwind_query(self.meta,res))
        return res


class BackendDataServer(BackendDataServer0):
    '''A new Redis backend with transactions'''
    
    def unwind_query(self, meta, qset):
        '''Unwind queryset'''
        table = meta.table()
        ids = list(qset)
        make_object = self.make_object
        for id,data in zip(ids,table.mget(ids)):
            yield make_object(meta,id,data)            
    
    def instance_keys(self, obj):
        meta = obj._meta
        if meta.multifields:
            keys = []
            for field in meta.multifields:
                f = getattr(obj,field.attname)
                keys.append(f.id)
            return keys
        else:
            return ()
    
    def make_object(self, meta, id , data):
        obj = meta.maker()
        obj.__setstate__((id,data))
        return obj
    
    def get_object(self, meta, name, value):
        if name != 'id':
            id = self._get(meta.basekey(name,value))
        else:
            id = value
        if id is None:
            raise ObjectNotFound
        hash = meta.table()
        data = hash.get(id)
        if data is None:
            raise ObjectNotFound
        return self.make_object(meta, id, data)
    
    def transaction(self, pipelined = True, cachepipes = None):
        '''Return a transaction instance'''
        return Transaction(self,
                           pipelined,
                           cachepipes)
        
    def save_object(self, obj, transaction = None):
        commit = False
        if not transaction:
            commit = True
            transaction = self.transaction(cachepipes = obj._cachepipes)
            
        # Save the object in the back-end
        meta = obj._meta
        timeout = meta.timeout
        if not obj.is_valid():
            raise FieldError(json.dumps(obj.errors))
        data = obj.cleaned_data
        objid = obj.id
        #
        # if editing (id already available) we need to clear the previous element.
        # But not its related objects.
        if objid:
            try:
                pobj = obj.__class__.objects.get(id = objid)
                self.delete_object(pobj, transaction, multi_field = False)
            #TODO: we should use this except but it fails ManyToMany field to fail tests
            #except obj.DoesNotExist:
            except:
                pass
        objid = obj.id = meta.pk.serialize(objid)
        
        # Add object data to the model hash table
        hash = meta.table(transaction)
        #hash.addnx(objid, data)
        hash.add(objid, data)
        bkey = meta.basekey
        
        # Add id to id set
        index = self.unordered_set(bkey('id'), timeout, pickler = nopickle, transaction = transaction)
        index.add(objid)
        
        # Create indexes if possible
        for field,value in obj.indices:
            if field.unique:
                key = bkey(field.name)
                index = self.hash(key,timeout,pickler=nopickle,transaction=transaction)
                index.add(value,objid)
            elif field.index:
                key = bkey(field.name,value)
                if meta.order_by:
                    index = self.ordered_set(key,
                                             timeout,
                                             pickler = ordering_pickle,
                                             scorefun = meta.order_by.scoreobject,
                                             transaction = transaction)
                    index.add(obj)
                else:
                    index = self.unordered_set(key, timeout, pickler = nopickle, transaction = transaction)
                    index.add(objid)
            # The hash table for ordering
            if field.ordered and field is not meta.order_by:
                key = bkey(field.name,'id')
                index = self.hash(key, timeout, pickler = nopickle,
                                  transaction = transaction)
                index.add(objid,value)
                
        if commit:
            transaction.commit()
        
        return obj
    
    def delete_object(self, obj, transaction = None, deleted = None, multi_field = True):
        commit = False
        if not transaction:
            commit = True
            transaction = self.transaction()
            
        deleted = deleted if deleted is not None else []
        append  = deleted.append
        meta    = obj._meta
        timeout = meta.timeout
        bkey    = meta.basekey
        objid   = obj.id
        pipe    = transaction.pipe

        # Remove object from model hash table
        # if not hash.delete(objid):
        #    return 0
        
        # ids set
        sid = bkey('id')
        index = self.unordered_set(sid, timeout, pickler = nopickle, transaction = transaction)
        index.discard(objid)
        #append(sid)
        
        for field in meta.fields:
            name = field.name
            if field.index:
                value = getattr(obj,name,None)
                if field.unique:
                    if field.primary_key:
                        key = bkey()
                    else:
                        key = bkey(name)
                    index = self.hash(key,timeout,pickler=nopickle,transaction=transaction)
                    index.delete(value)
                else:
                    key = bkey(name,field.serialize(value))
                    if meta.order_by:
                        index = self.ordered_set(key, timeout, pickler = nopickle, transaction = transaction)
                    else:
                        index = self.unordered_set(key, timeout, pickler = nopickle, transaction = transaction)
                    index.discard(objid)
            if field.ordered:
                key = bkey(field.name,'id')
                index = self.hash(key, timeout, transaction = transaction)
                index.delete(objid)
                
            fid = field.id(obj)
            if fid and multi_field:
                pipe.delete(fid)
                append(fid)
        
        if commit:
            transaction.commit()
            
        return 1
