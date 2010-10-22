from stdnet import structures


class List(structures.List):
    pass

class Set(structures.Set):
    
    def size(self):
        '''Size of set'''
        return self.cursor.execute_command('SCARD', self.id)
    
    def add(self, value):
        return self.cursor.execute_command('SADD', self.id, value)
    
    def _all(self):
        return self.cursor.execute_command('SMEMBERS', self.id)
    

class HashTable(structures.HashTable):
    
    def size(self):
        return len(self.doc)
    
    def get(self, key):
        return self.doc.get(key)
    
    def mget(self, keys):
        if not keys:
            raise StopIteration
        for key in keys:
            yield self.doc.get(key)
    
    def add(self, key, value):
        self.doc[key] = value
    
    def update(self, mapping):
        return self.cursor.hmset(self.id,mapping)
    
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
    
    
class Map(HashTable):
    
    def _keys(self, keys, desc):
        ks = [int(v) for v in keys]
        ks.sort()
        if desc:
            return reversed(ks)
        else:
            return ks
        
    def keys(self, desc = False):
        ks = super(Map,self).keys()
        return self._keys(ks,desc)

    def items(self, desc = False):
        kv   = self.cursor.execute_command('HGETALL', self.id)
        keys = self._keys(kv.keys(),desc)
        loads = self.cursor._res_to_val
        for key in keys:
            yield key,loads(kv[str(key)])