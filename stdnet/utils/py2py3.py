import sys
import types

__all__ = ['string_type',
           'int_type',
           'to_bytestring',
           'to_string',
           'ispy3k',
           'is_string',
           'iteritems',
           'itervalues',
           'pickle',
           'map',
           'zip',
           'range',
           'UnicodeMixin']


def ispy3k():
    return int(sys.version[0]) >= 3


if ispy3k(): # Python 3
    import pickle
    string_type = str
    itervalues = lambda d : d.values()
    iteritems = lambda d : d.items()
    is_string = lambda x : isinstance(x,str)
    zip = zip
    map = map
    range = range
    
    class UnicodeMixin(object):
        
        def __unicode__(self):
            return '{0} object'.format(self.__class__.__name__)
        
        def __str__(self):
            return self.__unicode__()
        
        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__,self)
    
else: # Python 2
    from itertools import izip as zip
    from itertools import imap as map
    import cPickle as pickle
    range = xrange
    string_type = unicode
    itervalues = lambda d : d.itervalues()
    iteritems = lambda d : d.iteritems()
    is_string = lambda x : isinstance(x,basestring)
    
    class UnicodeMixin(object):
        
        def __unicode__(self):
            return unicode('{0} object'.format(self.__class__.__name__))
        
        def __str__(self):
            return self.__unicode__().encode()
        
        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__,self)
    
try:
    int_type = (types.IntType, types.LongType)
except AttributeError:
    int_type = int
    
    
    
def to_bytestring(s, encoding='utf-8', errors='strict'):
    """Returns a bytestring version of 's',
encoded as specified in 'encoding'.
If strings_only is True, don't convert (some)
non-string-like objects."""
    if isinstance(s,bytes):
        if encoding != 'utf-8':
            return s.decode('utf-8', errors).encode(encoding, errors)
        else:
            return s
        
    if not is_string(s):
        s = string_type(s)
    return s.encode(encoding, errors)


def to_string(s, encoding='utf-8', errors='strict'):
    """Inverse of to_bytestring"""
    if isinstance(s, string_type):
        return s
    elif isinstance(s,bytes):
        return s.decode(encoding,errors)
    else:
        return string_type(s)
        