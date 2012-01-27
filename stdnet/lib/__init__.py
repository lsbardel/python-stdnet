#First try local
try:
    from .hr import *
    hasextensions = True
except ImportError:
    # Try Global
    try:
        from hr import *
        hasextensions = True
    except ImportError:
        hasextensions = False
        from .fallback import *

from . import fallback

#TODO
#For now we use the pure python implementation.
skiplist = fallback.skiplist
zset = fallback.zset