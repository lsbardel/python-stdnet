'''Classes used for encoding and decoding :class:`stdnet.odm.Field` values.


.. autoclass:: Encoder
   :members:
   :member-order: bysource


These are all available :class:`Encoder`:

.. autoclass:: NoEncoder

.. autoclass:: Default

.. autoclass:: NumericDefault

.. autoclass:: Bytes

.. autoclass:: Json
   
.. autoclass:: PythonPickle

.. autoclass:: DateTimeConverter
   
.. autoclass:: DateConverter
'''
import json
import logging

from datetime import datetime, date
from struct import pack, unpack

from stdnet.utils import JSONDateDecimalEncoder, pickle, \
                         JSONDateDecimalEncoder, DefaultJSONHook,\
                         ispy3k, date2timestamp, timestamp2date,\
                         string_type

nan = float('nan')

LOGGER = logging.getLogger('stdnet.encoders')


class Encoder(object):
    '''Virtaul class for encoding data in
:ref:`data structures <model-structures>`. It exposes two methods
for encoding and decoding data to and from the data server.

.. attribute:: type

    The type of data once loaded into python
'''
    type = None
    
    def dumps(self, x):
        '''Serialize data for database'''
        raise NotImplementedError()
    
    def loads(self, x):
        '''Unserialize data from database'''
        raise NotImplementedError()
    
    def require_session(self):
        '''``True`` if this :class:`Encoder` requires a
:class:`stdnet.odm.Session`.'''
        return False
    
    def load_iterable(self, iterable, session=None):
        '''Load an ``iterable``. By default it returns a generator of
data loaded via the :meth:`loads` method.

:param iterable: an iterable over data to load.
:param session: Optional :class:`stdnet.odm.Session`.
:return: an iterable over decoded data.
'''
        data = []
        load = self.loads
        for v in iterable:
            data.append(load(v))
        return data
    
        
class Default(Encoder):
    '''The default unicode encoder. It converts bytes to unicode when loading
data from the server. Viceversa when sending data.'''
    type = string_type
    
    def __init__(self, charset='utf-8', encoding_errors='strict'):
        self.charset = charset
        self.encoding_errors = encoding_errors
        
    if ispy3k:
        def dumps(self, x):
            if isinstance(x,bytes):
                return x
            else:
                return str(x).encode(self.charset,self.encoding_errors)
            
        def loads(self, x):
            if isinstance(x, bytes):
                return x.decode(self.charset,self.encoding_errors)
            else:
                return str(x)
    else:  # pragma nocover
        def dumps(self, x):
            if not isinstance(x,unicode):
                x = str(x)
            return x.encode(self.charset,self.encoding_errors)
            
        def loads(self, x):
            if not isinstance(x,unicode):
                x = str(x)
                return x.decode(self.charset,self.encoding_errors)
            else:
                return x
    

def safe_number(v): 
    try: 
        v = float(v)
        vi = int(v)
        return vi if vi == v else v 
    except: 
        return v 


class NumericDefault(Default):
    '''It decodes values into unicode unless they are numeric, in which case
they are decoded as such.'''
    def loads(self, x):
        x = super(NumericDefault, self).loads(x)
        return safe_number(x)
        
    
class Double(Encoder):
    '''It decodes values into doubles. If the decoding fails it decodes the
value into ``nan`` (not a number).'''
    type = float
    
    def loads(self, x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return nan
    dumps = loads
    
    
class Bytes(Encoder):
    '''The binary encoder'''
    type = bytes
    
    def __init__(self, charset = 'utf-8', encoding_errors = 'strict'):
        self.charset = charset
        self.encoding_errors = encoding_errors
        
    def dumps(self, x):
        if not isinstance(x,bytes):
            x = x.encode(self.charset,self.encoding_errors)
        return x
    
    loads = dumps


class NoEncoder(Encoder):
    '''A dummy encoder class'''
    def dumps(self, x):
        return x
    
    def loads(self, x):
        return x
    
    
class PythonPickle(Encoder):
    '''A safe pickle serializer. By default we use protocol 2 for compatibility
between python 2 and python 3.'''
    type = bytes
    
    def __init__(self, protocol = 2):
        self.protocol = protocol
        
    def dumps(self, x):
        if x is not None:
            try:
                return pickle.dumps(x,self.protocol)
            except:
                LOGGER.exception('Could not pickle %s', x)
    
    def loads(self, x):
        if x is None:
            return x
        elif isinstance(x, bytes):
            try:
                return pickle.loads(x)
            except (pickle.UnpicklingError, EOFError, ValueError):
                return x.decode('utf-8', 'ignore')
        else:
            return x
    

class Json(Default):
    '''A JSON encoder for maintaning python types when dealing with
remote data structures.'''
    def __init__(self,
                 charset = 'utf-8',
                 encoding_errors = 'strict',
                 json_encoder = None,
                 object_hook = None):
        super(Json,self).__init__(charset, encoding_errors)
        self.json_encoder = json_encoder or JSONDateDecimalEncoder
        self.object_hook = object_hook or DefaultJSONHook
        
    def dumps(self, x):
        return json.dumps(x, cls=self.json_encoder)
    
    def loads(self, x):
        if isinstance(x, bytes):
            x = x.decode(self.charset, self.encoding_errors)
        return json.loads(x, object_hook=self.object_hook)


class DateTimeConverter(Encoder):
    '''Convert to and from python ``datetime`` objects and unix timestamps'''
    type = datetime
    
    def dumps(self, value):
        return date2timestamp(value)
    
    def loads(self, value):
        return timestamp2date(value)
    

class DateConverter(DateTimeConverter):
    type = date
    '''Convert to and from python ``date`` objects and unix timestamps'''
    
    def loads(self, value):
        return timestamp2date(value).date()
    
    
class CompactDouble(Encoder):
    type = float
    nil = b'\x00'*8
    nan = float('nan')
            
    def dumps(self, value):
        if value is None:
            return self.nil
        value = float(value)
        if value != value:
            return self.nil
        else:
            return pack('>d', value)
    
    def loads(self, value):
        if value == self.nil:
            return self.nan
        else:
            return unpack('>d', value)[0]
    