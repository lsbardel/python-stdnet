
from stdnet import structures


class Set(structures.Set):
    
    def size(self):
        '''Size of set'''
        s = self._all()
        if s is None:
            return 0
        else:
            return len(s)
    
    def add(self, value):
        s = self._all()
        if s is None:
            s = set()
        s.add(value)
        self.cursor.cas(self.id,s)
    
    def _all(self):
        return self.cursor.get(self.id)
    

class HashTable(structures.HashTable):
    
    def size(self):
        return self.cursor.hlen(self.id)
    
    def get(self, key):
        return self.cursor.hget(self.id,key)
    
    def mget(self, keys):
        '''Get multiple keys'''
        if not keys:
            raise StopIteration
        objs = self.cursor.execute_command('HMGET', self.id, *keys)
        loads = self.cursor._res_to_val
        for obj in objs:
            yield loads(obj)
    
    def add(self, key, value):
        return self.cursor.set('%s:%s' % (self.id,key),value)
    
    def update(self, mapping):
        return self.cursor.hmset(self.id,mapping)
    
    def delete(self, key):
        return self.cursor.execute_command('HDEL', self.id, key)
    
    def keys(self):
        return self.cursor.execute_command('HKEYS', self.id)
    
    def items(self):
        result = self.cursor.execute_command('HGETALL', self.id)
        loads  = self.cursor._res_to_val
        for key,val in result.iteritems():
            yield key,loads(val)

    def values(self):
        for ky,val in self.items():
            yield val
