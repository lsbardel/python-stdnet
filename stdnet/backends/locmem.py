'''Thread-safe in-memory cache backend.
'''

import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

from stdnet.backends.base import BaseBackend, novalue
from stdnet.utils import RWLock, OrderedDict

class dummyPickle():
    
    def loads(self, data):
        return data

    def dumps(self, obj):
        return obj
    
class cache(object):
    
    def __init__(self):
        self.cache = {}
        self.expire_info = {}
    
    def clear(self):
        self.cache.clear()
        self.expire_info.clear()

_cache = cache()
    

class BackEnd(BaseBackend):
    
    def __init__(self, name, _, params):
        super(BackEnd,self).__init__(name,params)
        try:
            dopickle = int(params.get('pickle', 1))
        except:
            dopickle = 1
        if dopickle:
            self.pickle = pickle
        else:
            self.pickle = dummyPickle()
        max_entries = params.get('max_entries', 300)
        self._cache       = _cache.cache
        self._expire_info = _cache.expire_info
        try:
            self._max_entries = int(max_entries)
        except (ValueError, TypeError):
            self._max_entries = 300

        cull_frequency = params.get('cull_frequency', 3)
        try:
            self._cull_frequency = int(cull_frequency)
        except (ValueError, TypeError):
            self._cull_frequency = 3

        self._lock = RWLock()

    def _len(self, id):
        self._lock.reader_enters()
        try:
            sset = self._cache.get(id,None)
            if sset:
                return len(sset)
            else:
                return 0
        finally:
            self._lock.reader_leaves()
            
    def incr(self, key, delta=1):
        self._lock.writer_enters()
        try:
            val = self._cache.get(key,0) + delta
            self._cache[key] = val
            return val
        finally:
            self._lock.writer_leaves()
            
    def add(self, key, value, timeout=None):
        self._lock.writer_enters()
        try:
            exp = self._expire_info.get(key)
            if exp is None or exp <= time.time():
                try:
                    self._set(key, self.pickle.dumps(value), timeout)
                    return True
                except pickle.PickleError:
                    pass
            return False
        finally:
            self._lock.writer_leaves()

    def get(self, key, default=None):
        self._lock.reader_enters()
        try:
            exp = self._expire_info.get(key)
            if exp is None:
                return default
            elif exp == 0 or exp > time.time():
                try:
                    return self.pickle.loads(self._cache[key])
                except pickle.PickleError:
                    return default
        finally:
            self._lock.reader_leaves()
        self._lock.writer_enters()
        try:
            try:
                del self._cache[key]
                del self._expire_info[key]
            except KeyError:
                pass
            return default
        finally:
            self._lock.writer_leaves()

    def _set(self, key, value, timeout=None):
        if len(self._cache) >= self._max_entries:
            self._cull()
        if timeout is None:
            timeout = self.default_timeout
        self._cache[key] = value
        if not timeout:
            self._expire_info[key] = 0
        else:
            self._expire_info[key] = time.time() + timeout

    def set(self, key, value, timeout=None):
        self._lock.writer_enters()
        try:
            self._set(key, self.pickle.dumps(value), timeout)
            #except pickle.PickleError:
        finally:
            self._lock.writer_leaves()

    def has_key(self, key):
        self._lock.reader_enters()
        try:
            exp = self._expire_info.get(key)
            if exp is None:
                return False
            elif exp > time.time():
                return True
        finally:
            self._lock.reader_leaves()

        self._lock.writer_enters()
        try:
            try:
                del self._cache[key]
                del self._expire_info[key]
            except KeyError:
                pass
            return False
        finally:
            self._lock.writer_leaves()

    def _cull(self):
        if self._cull_frequency == 0:
            self.clear()
        else:
            doomed = [k for (i, k) in enumerate(self._cache) if i % self._cull_frequency == 0]
            for k in doomed:
                self._delete(k)

    def delete(self, *keys):
        self._lock.writer_enters()
        try:
            for key in keys:
                self._cache.pop(key,None)
                self._expire_info.pop(key,None)
        finally:
            self._lock.writer_leaves()

    def clear(self):
        global _cache
        _cache.clear()
        
    # Sets            
        
    def sadd(self, key, members):
        self._lock.writer_enters()
        try:
            sset = self._cache.get(key,None)
            if sset is None:
                sset = set()
                self._cache[key] = sset
                
            N = len(sset)
            if hasattr(members,'__iter__'):
                for member in members:
                    sset.add(member)
            else:
                sset.add(members)
            return len(sset) > N
        finally:
            self._lock.writer_leaves()
    
    def smembers(self,key):
        return self._cache.get(key,None)
    
    # Ordered sets
    def zadd(self, key, value, score = novalue):
        self._lock.writer_enters()
        try:
            if score == novalue:
                score = value
            sset = self._cache.get(key,None)
            if sset is None:
                sset = OrderedSet()
                self._cache[key] = sset
                
            N = len(sset)
            sset.add(value,score)
            return len(sset) > N
        finally:
            self._lock.writer_leaves()
    
    def zrange(self, key, start, end):
        self._lock.reader_enters()
        try:
            sset = self._cache.get(key,None)
            if sset:
                return sset.range(start,end)
            else:
                return None
        finally:
            self._lock.reader_leaves()
    
    def zlen(self, id):
        return self._len(id)
        
    # Hashes
    def hset(self, key, field, value):
        self._lock.writer_enters()
        try:
            sset = self._cache.get(key,None)
            if sset is None:
                sset = {}
                self._cache[key] = sset
                
            N = len(sset)
            sset[field] = value
            return len(sset) > N
        finally:
            self._lock.writer_leaves()
            
    # Map
    def map(self, id, typ = None):
        self._lock.writer_enters()
        try:
            mmap = self._cache.get(id,None)
            if mmap is None:
                mmap = OrderedDict()
                self._cache[id] = mmap
            return mmap
        finally:
            self._lock.writer_leaves()
    