import sys
from collections import *

if sys.version_info < (2,7):    # pragma nocover
    from .fallbacks._collections import *
    