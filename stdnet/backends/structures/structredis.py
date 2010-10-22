'''Two different implementation of a redis::map, a networked
ordered associative container
'''
import base as structures


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
        id = self.id
        s  = 0
        for value in self._pipeline.back:
            s = self.cursor.execute_command('RPUSH', id, value)
        for value in self._pipeline.front:
            s = self.cursor.execute_command('LPUSH', id, value)
        return s
        

class Set(structures.Set):
    
    def _size(self):
        '''Size of set'''
        return self.cursor.execute_command('SCARD', self.id)
    
    def delete(self):
        return self.cursor.execute_command('DEL', self.id)
    
    def discard(self, elem):
        return self.cursor.execute_command('SREM', self.id, elem)
    
    def _save(self):
        id = self.id
        s  = 0
        for value in self._pipeline:
            s += self.cursor.execute_command('SADD', id, value)
        return s
    
    def _contains(self, value):
        return self.cursor.execute_command('SISMEMBER', self.id, value)
    
    def _all(self):
        return self.cursor.execute_command('SMEMBERS', self.id)
    
    
class OrderedSet(structures.OrderedSet):
    
    def _size(self):
        '''Size of set'''
        return self.cursor.execute_command('ZCARD', self.id)
    
    def discard(self, elem):
        return self.cursor.execute_command('ZREM', self.id, elem)
    
    def _contains(self):
        return self.cursor.execute_command('ZSCORE', self.id, value) is not None
    
    def _all(self):
        return self.cursor.redispy.zrange(self.id, 0, -1)
    
    def _save(self):
        id = self.id
        s  = 0
        for score,value in self._pipeline:
            s += self.cursor.execute_command('ZADD', id, score, value)
        return s


class HashTable(structures.HashTable):
    
    def _size(self):
        return self.cursor.execute_command('HLEN', self.id)
    
    def delete(self):
        return self.cursor.execute_command('DEL', self.id)
    
    def _get(self, key):
        return self.cursor.execute_command('HGET', self.id, key)
    
    def _mget(self, keys):
        return self.cursor.execute_command('HMGET', self.id, *keys)
    
    def delete(self, key):
        return self.cursor.execute_command('HDEL', self.id, key)
    
    def _keys(self):
        return self.cursor.execute_command('HKEYS', self.id)
    
    def _items(self):
        return self.cursor.execute_command('HGETALL', self.id)

    def values(self):
        for ky,val in self.items():
            yield val
            
    def _save(self):
        items = []
        [items.extend(item) for item in self._pipeline.iteritems()]
        return self.cursor.execute_command('HMSET',self.id,*items)
    
