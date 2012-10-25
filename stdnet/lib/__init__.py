try:
    from . import hr
    hasextensions = True
except:
    hasextensions = False
    hr = None
    
from . import fallback
skiplist = fallback.skiplist
zset = fallback.zset