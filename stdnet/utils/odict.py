from copy import copy
from bisect import bisect, insort

_novalue = object()

class OrderedSet(object):
    
    def __init__(self, init_val = None):
        super(OrderedSet,self).__init__()
        if isinstance(init_val,self.__class__):
            self._sequence = copy(init_val._sequence)
            self._set      = copy(init_val._set)
        else:
            self._sequence = []
            self._set      = {}
            if init_val:
                for v in init_val:
                    self.add(v)
    
    def __iter__(self):
        return self._sequence.__iter__()
    
    def __len__(self):
        return len(self._set)
        
    def add(self, value, score):
        sc = self._set.get(value,_novalue)
        if sc == _novalue:
            self._set[value] = score
        if value in self._set:
            pass
        #insort(self._sequence, score)
        #self._set.add(val)    
    
    def range(self, start, end):
        s = self._sequence
        v1 = bisect(s,start)-1
        v2 = bisect(s,end)-1
        return self._sequence[v1:v2]

class OrderedDict(dict):
    
    def __init__(self, init_val = None):
        super(OrderedDict,self).__init__()
        self._sequence = []
        self.update(init_val)
    
    def keys(self):
        return self._sequence
        
    def __iter__(self):
        return self._sequence.__iter__()
    
    def items(self):
        for key in self:
            yield key, self.get(key)
    
    def values(self):
        for key in self:
            yield self.get(key)
    
    def add(self, key, value):
        added = 0
        if not self.has_key(key):
            added = 1
            insort(self._sequence, key)
        self._set(key,value)
        return added
    
    def _set(self, key, val):
        super(OrderedDict,self).__setitem__(key,val)
        
    def __setitem__(self, key, val):
        self.add(key,val)
    
    def pop(self, key, default = None):
        if super(OrderedDict,self).pop(key,_novalue) == _novalue:
            return default
        s = self._sequence
        return s.pop(bisect(s,key)-1)
    
    def update(self, init_val):
        if not init_val:
            return
        if isinstance(init_val,dict):
            init_val = init_val.items()
        for k,v in init_val:
            self[k] = v
        
    def range(self, start, end):
        s = self._sequence
        v1 = bisect(s,start)-1
        v2 = bisect(s,end)-1
        seq = self._sequence[v1:v2]
        for key in seq:
            yield key,self[key]
        