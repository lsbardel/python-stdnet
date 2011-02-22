try:
    from stdlib import *
    hasextensions = True
except:
    hasextensions = False
    from .fallback import *
else:
    from stdnet.lib import fallback