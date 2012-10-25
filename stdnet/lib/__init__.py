try:
    import hiredis
except:
    hiredis = None
hiredis = None
from . import fallback
skiplist = fallback.skiplist
zset = fallback.zset