from uuid import uuid4
import json

from stdnet.utils import zip, iteritems, to_bytestring
from stdnet.exceptions import FieldError, ObjectNotFound

from .base import nopickle
from .redisb import BackendDataServer as BackendDataServer0

def gen_unique_id():
    return str(uuid4())


class Transaction(object):
    
    def __init__(self, server, pipelined = True, cachepipes = None):
        self.server = server
        if pipelined:
            self.pipe = server.redispy.pipeline()
        else:
            self.pipe = server.redispy
        self._cachepipes = cachepipes if cachepipes is not None else {}
        self._keys = {}
        
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
        keys = self._keys
        pipe = self.pipe
        for id,cachepipe in iteritems(cachepipes):
            el = getattr(self.server,cachepipe.method)(id, transaction = self)
            el.save()
        if keys: 
            self._set_keys(keys, transaction = self)
        self._keys.clear()
        
        if hasattr(pipe,'execute'):
            pipe.execute()
       
    def __enter__(self):
        return self
     
    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()
        


class BackendDataServer(BackendDataServer0):
    '''A new Redis backend with transactions'''
    
    def unwind_query(self, meta, qset):
        table = meta.table()
        ids = list(qset)
        make_object = self.make_object
        for id,data in zip(ids,table.mget(ids)):
            yield make_object(meta,id,data)            
        
    def idset(self, meta):
        return self.unordered_set(meta.basekey('id'), pickler = nopickle)
    
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
    
    def _unique_set(self, meta, idset, name, values, check = True):
        '''Handle filtering over unique fields'''
        uset = set()
        if name == 'id':
            for id in values:
                if not check or id in idset:
                    uset.add(to_bytestring(id))
        else:
            for value in values:
                key = meta.basekey(name)
                id = self.hash(key, pickler = nopickle).get(value)
                if id:
                    uset.add(to_bytestring(id))
        return uset
        
    def query(self, meta, fargs, eargs, filter_sets = None):
        # QUERY a model
        #
        # fargs is a dictionary of filters
        # eargs is a dictionary of excludes
        # filter_sets are ids from fields
        #
        qset = None
        temp_ids = []
        idset = self.idset(meta)
            
        filters = None
        if fargs:
            filters = []
            for name,data in iteritems(fargs):
                values,unique = data
                if unique:
                    uset = self._unique_set(meta, idset, name, values)
                    if not uset:
                        return uset
                    if qset is None:
                        qset = uset
                    else:
                        qset = qset.intersection(uset)
                        if not qset:
                            return qset
                elif values:
                    if len(values) == 1:
                        filters.append(meta.basekey(name,values[0]))
                    else:
                        insersept = [meta.basekey(name,value) for value in values]
                        id = gen_unique_id()
                        temp_ids.append(id)
                        self.sunionstore(id,insersept)
                        filters.append(id)
                    
        if filters or filter_sets:
            if filters and filter_sets:
                filters.extend(filter_sets)
            elif not filters:
                filters = filter_sets
                
            v = self.sinter(filters)
            if qset:
                qset.intersection(v)
            else:
                qset = v
            
            if not qset:
                return qset
            
        if eargs:
            excludes = []
            euset = set()
            for name,data in iteritems(eargs):
                values,unique = data
                if unique:
                    euset = euset.union(self._unique_set(meta, idset, name, values, check = False))
                else:
                    if len(values) == 1:
                        excludes.append(meta.basekey(name,values[0]))
                    else:
                        insersept = [meta.basekey(name,value) for value in values]
                        id = gen_unique_id()
                        temp_ids.append(id)
                        self.sunionstore(id,insersept)
                        excludes.append(id)
                        
            if excludes:
                excludes.insert(0,idset.id)
                eset  = self.sdiff(excludes)
                if qset:
                    qset = qset.intersection(eset)
                else:
                    qset = eset
            elif qset is None:
                qset = set(idset)
                
            if euset:
                qset -= euset
        
        if qset is None:
            qset = idset
            
        if temp_ids:
            self.delete(*temp_ids)
        return qset
    
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
                #index = self.index_keys(key, timeout, transaction = transaction)
            else:
                key = bkey(field.name,value)
                if field.ordered:
                    index = self.ordered_set(key, timeout, pickler = nopickle, transaction = transaction)
                else:
                    index = self.unordered_set(key, timeout, pickler = nopickle, transaction = transaction)
                index.add(objid)
                
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
                    if field.ordered:
                        index = self.ordered_set(key, timeout, pickler = nopickle, transaction = transaction)
                    else:
                        index = self.unordered_set(key, timeout, pickler = nopickle, transaction = transaction)
                    index.discard(objid)
            fid = field.id(obj)
            if fid and multi_field:
                pipe.delete(fid)
                append(fid)
        
        if commit:
            transaction.commit()
            
        return 1
