'''Two different implementation of a redis::map, a networked
ordered associative container
'''
from stdnet.utils import iteritems
from stdnet.backends.structures import base as structures

    
class CommonMixin(object):
    '''Implements common functions to all Redis structures.'''
    def _delete(self, cursor):
        return cursor.execute_command('DEL', self.id)
    
    def add_expiry(self, cursor):
        cursor.execute_command('EXPIRE', self.id, self.timeout)


class List(CommonMixin,structures.List):
    
    def _size(self, cursor):
        '''Size of map'''
        return cursor.execute_command('LLEN', self.id)
    
    def _pop_back(self, cursor):
        return cursor.execute_command('RPOP', self.id)
    
    def _pop_front(self, cursor):
        return cursor.execute_command('LPOP', self.id)
    
    def _block_pop_back(self, cursor, timeout = 0):
        return cursor.execute_command('BRPOP', self.id, timeout)
    
    def _block_pop_front(self, cursor, timeout = 0):
        return cursor.execute_command('BLPOP', self.id, timeout)
    
    def _all(self, cursor):
        return cursor.execute_command('LRANGE', self.id, 0, -1)
    
    def _save(self, cursor, pipeline):
        s  = 0
        id = self.id
        c = cursor.execute_command
        b,f=None,None
        # Redis 2.4
        #if pipeline.back:
        #    b = c('RPUSH', id, pipeline.back)
        #if pipeline.front:
        #    f = c('LPUSH', id, pipeline.front)
        #return b,f
        for value in pipeline.back:
            c('RPUSH', id, value)
        for value in pipeline.front:
            c('LPUSH', id, value)
        
        
class Set(CommonMixin, structures.Set):
    
    def _size(self, cursor):
        '''Size of set'''
        return cursor.execute_command('SCARD', self.id)
    
    def _remove(self, cursor, values):
        return cursor.execute_command('SREM', self.id, *values)
    
    def _save(self, cursor, pipeline):
        return cursor.execute_command('SADD', self.id, *pipeline)
    
    def _has(self, cursor, value):
        return cursor.execute_command('SISMEMBER', self.id, value)
    
    def _all(self, cursor):
        return cursor.execute_command('SMEMBERS', self.id)
    
    
class OrderedSet(CommonMixin,structures.OrderedSet):
    
    def _size(self, cursor):
        '''Size of set'''
        return cursor.execute_command('ZCARD', self.id)
    
    def _has(self, cursor, value):
        res = cursor.execute_command('ZSCORE', self.id, value)
        return False if res is None else res
    
    def _discard(self, cursor, elem):
        return cursor.execute_command('ZREM', self.id, elem)
    
    def _rank(self, cursor, elem):
        return cursor.execute_command('ZRANK', self.id, elem)
        
    def _all(self, cursor, desc = False, withscores = False):
        return self.range(cursor, 0, -1, desc = desc, withscores = withscores)
    
    def range(self, cursor, start, end = -1, desc = False, withscores = False):
        return cursor.execute_command('ZRANGE', self.id, start, end,
                                      desc = desc, withscores = withscores)
    
    def _save(self, cursor, pipeline):
        cursor.execute_command('ZADD', self.id, *pipeline.flat())


class HashTable(CommonMixin,structures.HashTable):
    
    def _size(self, cursor):
        return cursor.execute_command('HLEN', self.id)
    
    def _has(self, cursor, value):
        return True if cursor.execute_command('HEXISTS', self.id, value)\
                 else False
                 
    def _get(self, cursor, key):
        return cursor.execute_command('HGET', self.id, key)
    
    def _pop(self, cursor, key):
        return cursor.execute_command('HDEL', self.id, key)
        
    def _keys(self, cursor):
        return cursor.execute_command('HKEYS', self.id)
    
    def _items(self, cursor, keys):
        if keys:
            return cursor.execute_command('HMGET', self.id, *keys)
        else:
            return cursor.execute_command('HGETALL', self.id)
    
    def _save(self, cursor, pipeline):
        items = []
        [items.extend(item) for item in iteritems(pipeline)]
        cursor.execute_command('HMSET',self.id,*items)
        
    def _addnx(self, cursor, field, value):
        return cursor.execute_command('HSETNX', self.id, field, value)
        

class TS(CommonMixin,structures.TS):
    '''Requires Redis Map structure which is not yet implemented in redis
(and I don't know if it will).
It is implemented on my redis-fork at https://github.com/lsbardel/redis'''
    def _size(self, cursor):
        return cursor.execute_command('TSLEN', self.id)

    def _has(self, cursor, key):
        return cursor.execute_command('TSEXISTS', self.id, key)
    
    def _get(self, cursor, key):
        return cursor.execute_command('TSGET', self.id, key)
    
    def _pop(self, cursor, key):
        return cursor.execute_command('TSDEL', self.id, key)
        
    def _getdate(self, val):
        return None if not val else val[0]
        
    def _irange(self, cursor, start, end):
        return cursor.execute_command('TSRANGE', self.id, start, end,
                                      'withtimes', withtimes = True)
    
    def _range(self, cursor, start, end):
        return cursor.execute_command('TSRANGEBYTIME', self.id, start, end,
                                      'withtimes', withtimes = True)
    
    def _count(self, cursor, start, end):
        return cursor.execute_command('TSCOUNT', self.id, start, end)
    
    def _front(self, cursor):
        return cursor.execute_command('TSRANGE', self.id, 0, 0, 'novalues',
                                      novalues = True, single = True)
    
    def _back(self, cursor):
        return cursor.execute_command('TSRANGE', self.id, -1, -1, 'novalues',
                                      novalues = True, single = True)
        
    def _keys(self, cursor):
        return cursor.execute_command('TSRANGE', self.id, 0, -1, 'novalues',
                                      novalues = True)
    
    def _items(self, cursor, keys):
        if keys:
            return cursor.execute_command('TMGET', self.id, *keys)
        else:
            return cursor.execute_command('TSRANGE', self.id, 0, -1,
                                          'withtimes', withtimes = True)
            
    def _save(self, cursor, pipeline):
        items = []
        [items.extend(item) for item in iteritems(pipeline)]
        return cursor.execute_command('TSADD',self.id,*items)
    
