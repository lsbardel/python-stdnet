import sys

from .skiplist import skiplist

ispy3k = int(sys.version[0]) >= 3

__all__ = ['zset']

 
class zset(object):
    '''Ordered-set equivalent of redis zset.'''
    def __init__(self):
        self.clear()
    
    def __repr__(self):
        return repr(self._sl)
    
    def __str__(self):
        return str(self._sl)
    
    def __len__(self):
        return len(self._dict)
    
    def __iter__(self):
        for _, value in self._sl:
            yield value
            
    def items(self):
        '''Iterable over ordered score, value pairs of this :class:`zset`'''
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
        self._sl.insert(score, val)
        return r
    
    def update(self, score_vals):
        '''Update the :class:`zset` with an iterable over pairs of
scores and values.'''
        add = self.add
        for score, value in score_vals:
            add(score, value)
            
    def remove(self, item):
        '''Remove ``item`` for the :class:`zset` it it exists.
If found it returns the score of the item removed.'''
        score = self._dict.pop(item, None)
        if score is not None:
            self._sl.remove(score)
            return score
        
    def clear(self):
        '''Clear this :class:`zset`.'''
        self._sl = skiplist()
        self._dict = {}
    
    def rank(self, item):
        '''Return the rank (index) of ``item`` in this :class:`zset`.'''
        score = self._dict.get(item)
        if score is not None:
            return self._sl.rank(score)
            
    def flat(self):
        return self._sl.flat()
