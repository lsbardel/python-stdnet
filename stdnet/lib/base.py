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

#For now
zset = fallback.zset


    
