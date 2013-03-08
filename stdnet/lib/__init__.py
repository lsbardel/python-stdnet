try:
    import hiredis
except:
    hiredis = None
hiredis = None
from . import fallback
from .async import *
skiplist = fallback.skiplist
zset = fallback.zset