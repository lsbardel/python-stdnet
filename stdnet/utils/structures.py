import sys
from collections import *

if sys.version_info < (2,7):
    from .fallbacks._collections import *
    