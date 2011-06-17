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
        v = self.cursor.execute_command('RPOP', self.id)
        if v:
            return self.pickler.loads(v)
    
    def pop_front(self):
        v = self.cursor.execute_command('LPOP', self.id)
        if v:
            return self.pickler.loads(v)
    
    def block_pop_back(self, timeout = 0):
        v = self.cursor.execute_command('BRPOP', self.id, timeout)
        if v:
            return self.pickler.loads(v[1])
    
    def block_pop_front(self, timeout = 0):
        v = self.cursor.execute_command('BLPOP', self.id, timeout)
        if v:
            return self.pickler.loads(v[1])
    
    def _all(self):
        return self.cursor.execute_command('LRANGE', self.id, 0, -1)
    
    def _save(self):
        s  = 0
        id = self.id
        c = self.cursor.execute_command
        for value in self.pipeline.back:
            c('RPUSH', id, value)
        for value in self.pipeline.front:
            c('LPUSH', id, value)
    
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
    
    def _discard(self, elem):
        return self.cursor.execute_command('SREM', self.id, elem)
    
    def _save(self):
        id = self.id
        s  = 0
        cursor = self.cursor
        for value in self.pipeline:
            cursor = cursor.execute_command('SADD', id, value)
    
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
    
    def _discard(self, elem):
        return self.cursor.execute_command('ZREM', self.id, elem)
    
    def _contains(self, value):
        return self.cursor.execute_command('ZSCORE', self.id, value) is not None
    
    def _rank(self, elem):
        return self.cursor.execute_command('ZRANK', self.id, elem)
        
    def _all(self, desc = False, withscores = False):
        return self.range(0, -1, desc = desc, withscores = withscores)
    
    def range(self, start, end = -1, desc = False, withscores = False):
        return self.cursor.execute_command('ZRANGE', self.id, start, end,
                                           desc = desc,
                                           withscores = withscores)
    
    def _save(self):
        id = self.id
        s  = 0
        for score,value in self.pipeline:
            self.cursor.execute_command('ZADD', id, score, value)

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
        return self.cursor.execute_command('HGETALL', self.id)
        #return riteritems(self, 'HGETALL', self.id)

    def values(self):
        for ky,val in self.items():
            yield val
            
    def _save(self):
        items = []
        [items.extend(item) for item in iteritems(self.pipeline)]
        self.cursor.execute_command('HMSET',self.id,*items)
        
    def _addnx(self, field, value):
        return self.cursor.execute_command('HSETNX', self.id, field, value)
    
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
        