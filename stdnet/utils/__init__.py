from stdnet.lib.py2py3.py2py3 import *
if ispy3k:
    import pickle
    from io import BytesIO
    unichr = chr
else:
    import cPickle as pickle
    from cStringIO import StringIO as BytesIO
    unichr = unichr 
    

from .jsontools import *
from .populate import populate
from .fields import *
from .dates import *
    
    
class NoValue(object):
    
    def __repr__(self):
        return '<NoValue>'
    __str__ = __repr__
    

novalue = NoValue()

        
