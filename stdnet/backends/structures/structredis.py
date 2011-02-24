'''Two different implementation of a redis::map, a networked
ordered associative container
'''
from stdnet.utils import zip, iteritems
from stdnet.backends.structures import base as structures


def riteritems(self, com, *rargs):
    res = self.cursor.execute_command(com, self.id, *rargs)
    if res:
        return zip(res[::2], res[1::2])
    else:
        return res


class List(structures.List):
    
    def _size(self):
        '''Size of map'''
        return self.cursor.execute_command('LLEN', self.id)
    
    def delete(self):
        return self.cursor.execute_command('DEL', self.id)
    
    def pop_back(self):
        return self.pickler.loads(self.cursor.execute_command('RPOP', self.id))
    
    def pop_front(self):
        return self.pickler.loads(self.cursor.execute_command('LPOP', self.id))
    
    def _all(self):
        return self.cursor.execute_command('LRANGE', self.id, 0, -1)
    
    def _save(self):
        s  = 0
        for value in self.pipeline.back:
            s = self.cursor.execute_command('RPUSH', self.id, value)
        for value in self.pipeline.front:
            s = self.cursor.execute_command('LPUSH', self.id, value)
        return s
    
    def add_expiry(self):
        self.cursor.execute_command('EXPIRE', self.id, self.timeout)
        

class Set(structures.Set):
    
    def _size(self):
        '''Size of set'''
        return self.cursor.execute_command('SCARD', self.id)
    
    def delete(self):
        return self.cursor.execute_command('DEL', self.id)
    
    def clear(self):
        return self.delete()
    
    def discard(self, elem):
        return self.cursor.execute_command('SREM', self.id, elem)
    
    def _save(self):
        id = self.id
        s  = 0
        for value in self.pipeline:
            s += self.cursor.execute_command('SADD', id, value)
        return s
    
    def _contains(self, value):
        return self.cursor.execute_command('SISMEMBER', self.id, value)
    
    def _all(self):
        return self.cursor.execute_command('SMEMBERS', self.id)
    
    def add_expiry(self):
        self.cursor.execute_command('EXPIRE', self.id, self.timeout)
    
    
class OrderedSet(structures.OrderedSet):
    
    def _size(self):
        '''Size of set'''
        return self.cursor.execute_command('ZCARD', self.id)
    
    def discard(self, elem):
        return self.cursor.execute_command('ZREM', self.id, elem)
    
    def _contains(self, value):
        return self.cursor.execute_command('ZSCORE', self.id, value) is not None
    
    def _all(self):
        return self.cursor.redispy.zrange(self.id, 0, -1)
    
    def _save(self):
        id = self.id
        s  = 0
        for score,value in self.pipeline:
            s += self.cursor.execute_command('ZADD', id, score, value)
        return s

    def add_expiry(self):
        self.cursor.execute_command('EXPIRE', self.id, self.timeout)


class HashTable(structures.HashTable):
    
    def _size(self):
        return self.cursor.execute_command('HLEN', self.id)
    
    def clear(self):
        return self.cursor.execute_command('DEL', self.id)
    
    def _get(self, key):
        return self.cursor.execute_command('HGET', self.id, key)
    
    def _mget(self, keys):
        return self.cursor.execute_command('HMGET', self.id, *keys)
    
    def delete(self, key):
        return self.cursor.execute_command('HDEL', self.id, key)
    
    def _contains(self, value):
        if self.cursor.execute_command('HEXISTS', self.id, value):
            return True
        else:
            return False
        
    def _keys(self):
        return self.cursor.execute_command('HKEYS', self.id)
    
    def _items(self):
        return iteritems(self.cursor.execute_command('HGETALL', self.id))
        #return riteritems(self, 'HGETALL', self.id)

    def values(self):
        for ky,val in self.items():
            yield val
            
    def _save(self):
        items = []
        [items.extend(item) for item in iteritems(self.pipeline)]
        return self.cursor.execute_command('HMSET',self.id,*items)
    
    def add_expiry(self):
        self.cursor.execute_command('EXPIRE', self.id, self.timeout)
        

class TS(structures.TS):
    '''Requires Redis Map structure which is not yet implemented in redis (and I don't know if it will).
It is implemented on my redis-fork at https://github.com/lsbardel/redis'''
    def _size(self):
        return self.cursor.execute_command('TSLEN', self.id)
    
    def clear(self):
        return self.cursor.execute_command('DEL', self.id)
    
    def _get(self, key):
        return self.cursor.execute_command('TSGET', self.id, key)
    
    def _mget(self, keys):
        return self.cursor.execute_command('TMGET', self.id, *keys)
    
    def delete(self, key):
        return self.cursor.execute_command('TSDEL', self.id, key)
    
    def _contains(self, key):
        return self.cursor.execute_command('TSEXISTS', self.id, key)
        
    def _getdate(self, val):
        return None if not val else val[0]
        
    def _irange(self, start, end):
        return riteritems(self, 'TSRANGE', start, end, 'withtimes')
    
    def _range(self, start, end):
        return riteritems(self, 'TSRANGEBYTIME', start, end, 'withtimes')
    
    def _count(self, start, end):
        return self.cursor.execute_command('TSCOUNT', self.id, start, end)
    
    def _front(self):
        return self._getdate(self.cursor.execute_command('TSRANGE', self.id, 0, 0, 'novalues'))
    
    def _back(self):
        return self._getdate(self.cursor.execute_command('TSRANGE', self.id, -1, -1, 'novalues'))
        
    def _keys(self):
        return self.cursor.execute_command('TSRANGE', self.id, 0, -1, 'novalues')
    
    def _items(self):
        return riteritems(self, 'TSRANGE', 0, -1, 'withtimes')

    def values(self):
        for ky,val in self.items():
            yield val
            
    def _save(self):
        items = []
        [items.extend(item) for item in iteritems(self.pipeline)]
        return self.cursor.execute_command('TSADD',self.id,*items)
    
    def add_expiry(self):
        self.cursor.execute_command('EXPIRE', self.id, self.timeout)
        