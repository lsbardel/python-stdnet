'''
Memcached cache backend
'''
from itertools import imap
import time

from stdnet.backends.base import BaseBackend, ImproperlyConfigured, BadCacheDataStructure
from stdnet.utils import smart_str, json

try:
    import cmemcache as memcache
    import warnings
    warnings.warn(
        "Support for the 'cmemcache' library has been deprecated. Please use python-memcached instead.",
        PendingDeprecationWarning
    )
except ImportError:
    try:
        import memcache
    except:
        raise ImproperlyConfigured("Memcached cache backend requires either the 'memcache' or 'cmemcache' library")
    


setmemkey = lambda k,n: '%s:%s' % (k,n)    



class MemcachedMap(object):
    '''Simulate a std::map in memcached.
    Keys are integer values'''
    def __init__(self, id):
        self.id      = id
        self.minkey  = '%s:min' % id
        self.maxkey  = '%s:max' % id
        self.lenkey  = '%s:len' % id
        self.keyskey = id
    
    def keyid(self, key):
        return '%s:key:%s' % (self.id,key)
    
    def len(self, cache):
        return cache.get(self.lenkey)
    
    def ids(self, cache):
        keys = self.keys(cache,False)
        return tuple(keys)+(self.id,self.lenkey,self.keyskey,self.minkey,self.maxkey)
    
    def min(self, cache):
        return cache.get(self.minkey)
    
    def max(self, cache):
        return cache.get(self.maxkey)
        
    def keys(self, cache, so = True):
        try:
            ks = cache.get(self.keyskey)
            ks = ks.split(':')
            if so:
                ks.sort()
            return ks
        except:
            return []
          
    def add(self, cache, key, value):
        keyid  = self.keyid(key)
        mkey   = self.max(cache)
        added  = 1
        if mkey is None:
            cache.set(self.minkey,key)
            cache.set(self.maxkey,key)
            cache.set(self.lenkey,1)
            cache.set(self.keyskey,str(key))
        elif key > mkey:
            cache.set(self.maxkey,key)
            cache.incr(self.lenkey)
            cache.append(self.keyskey,':%s' % key)
        elif key < self.min(cache):
            cache.set(self.minkey,key)
            cache.incr(self.lenkey)
            cache.append(self.keyskey,':%s' % key)
        else:
            if cache.get(keyid) is None:
                cache.incr(self.lenkey)
                cache.append(self.keyskey,':%s' % key)
            else:
                added = 0
        cache.set(keyid,value)
        return added
                
    def range(self, cache, start = 0, end = -1, desc = False):
        keys = self.keys(cache)
        kid  = self.keyid
        for key in keys:
            yield int(key),cache.get(kid(key))
            

class BackEnd(BaseBackend):
    
    def __init__(self, name, server, params):
        super(BackEnd,self).__init__(name,params)
        self._cache = memcache.Client(server.split(';'))

    def _get_memcache_timeout(self, timeout):
        """
        Memcached deals with long (> 30 days) timeouts in a special
        way. Call this function to obtain a safe value for your timeout.
        """
        if timeout is None:
            timeout = self.default_timeout
        if timeout > 2592000: # 60*60*24*30, 30 days
            # See http://code.google.com/p/memcached/wiki/FAQ
            # "You can set expire times up to 30 days in the future. After that
            # memcached interprets it as a date, and will expire the item after
            # said date. This is a simple (but obscure) mechanic."
            #
            # This means that we have to switch to absolute timestamps.
            timeout += int(time.time())
        return timeout

    def add(self, key, value, timeout=None):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return self._cache.add(smart_str(key), value, self._get_memcache_timeout(timeout))

    def get(self, key, default=None):
        val = self._cache.get(smart_str(key))
        if val is None:
            return default
        return val

    def set(self, key, value, timeout=None):
        self._cache.set(smart_str(key), value, self._get_memcache_timeout(timeout))
        
    def append(self, key, value, timeout=None):
        self._cache.append(smart_str(key), value, self._get_memcache_timeout(timeout))

    def delete(self, *keys):
        km = []
        for key in keys:
            km.extend(MemcachedMap(key).ids(self))
        return self._cache.delete_multi(km)
        #return self._cache.delete_multi(map(smart_str, keys))

    def get_many(self, keys):
        return self._cache.get_multi(map(smart_str,keys))

    def close(self, **kwargs):
        self._cache.disconnect_all()

    def incr(self, key, delta=1):
        try:
            val = self._cache.incr(key, delta)

        # python-memcache responds to incr on non-existent keys by
        # raising a ValueError. Cmemcache returns None. In both
        # cases, we set value to 0 and call again.
        except ValueError:
            val = None
            
        if val is None:
            self.set(key,0)
            return self.incr(key, delta)

        else:
            return val

    def decr(self, key, delta=1):
        try:
            val = self._cache.decr(key, delta)

        # python-memcache responds to decr on non-existent keys by
        # raising a ValueError. Cmemcache returns None. In both
        # cases, we should raise a ValueError though.
        except ValueError:
            val = None
        if val is None:
            raise ValueError("Key '%s' not found" % key)
        return val

    def set_many(self, data, timeout=None):
        safe_data = {}
        for key, value in data.items():
            if isinstance(value, unicode):
                value = value.encode('utf-8')
            safe_data[smart_str(key)] = value
        self._cache.set_multi(safe_data, self._get_memcache_timeout(timeout))

    def clear(self):
        self._cache.flush_all()
        
    
    # Operations on SETS
    def __setmembers(self, key):
        '''Mimic a set in memcached'''
        skey    = smart_str(key)
        members = self.get(skey)
        if members:
            try:
                members = json.loads(members)
            except:
                raise BadCacheDataStructure('key %s does not contain a set' % key)
        else:
            members = []
        return skey,members
        
    def sadd(self, key, members):
        skey,emembers = self.__setmembers(key)
        if not hasattr(members,'__iter__'):
            members = [members]
        for member in members:
            if member not in emembers:
                emembers.append(member)
        self.set(skey,json.dumps(emembers))
        return len(emembers)
    
    def smembers(self,key):
        skey,emembers = self.__setmembers(key)
        return emembers
    
    # Map    
    def madd(self, id, key, value):
        return MemcachedMap(id).add(self,key,value)
    
    def mlen(self, id):
        return MemcachedMap(id).len(self._cache)
    
    def mrange(self, id, start = 0, end = -1, desc = False):
        return MemcachedMap(id).range(self._cache, start, end, desc = desc)
    