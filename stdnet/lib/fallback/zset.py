from .skiplist import skiplist

__all__ = ['zset']

 
class zset(object):
    
    def __init__(self):
        self.clear()
                
    def __len__(self):
        return len(self._dict)
    
    def __iter__(self):
        return iter(self._sl)
    
    def add(self, score, val):
        r = 1
        if val in self._dict:
            sc = self._dict[val]
            if sc == score:
                return 0
            self._sl.remove(sc)
            r = 0
        self._dict[val] = score
        self._sl.insert(score,val)
        return r
    
    def update(self, scorevals):
        add = self.add
        for score,value in scorevals:
            add(score,value)
            
    def clear(self):
        self._sl = skiplist()
        self._dict = {}
        
    def _flat(self):
        for el in self:
            yield el[0]
            yield el[1]
            
    def flat(self):
        return tuple(self._flat())