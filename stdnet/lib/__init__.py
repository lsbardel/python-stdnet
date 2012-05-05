#First try local
try:
    from . import hr
    hasextensions = True    # pragma nocover
except ImportError:
    # Try Global
    try:
        import hr
        hasextensions = True    # pragma nocover
    except ImportError:
        hasextensions = False
        hr = None
        from .fallback import *

from . import fallback
    
#TODO
#For now we use the pure python implementation.
skiplist = fallback.skiplist
zset = fallback.zset