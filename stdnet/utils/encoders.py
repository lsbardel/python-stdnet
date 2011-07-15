'''Classes used for encoding and decoding field values.'''
import json

from stdnet.utils import JSONDateDecimalEncoder, pickle, \
                         JSONDateDecimalEncoder, DefaultJSONHook,\
                         ispy3k
    
    
class Default(object):
    '''The default unicode encoder'''
    def __init__(self, charset = 'utf-8', encoding_errors = 'strict'):
        self.charset = charset
        self.encoding_errors = encoding_errors
        
    if ispy3k:
        def dumps(self, x):
            if isinstance(x,bytes):
                return x
            else:
                return str(x).encode(self.charset,self.encoding_errors)
            
        def loads(self, x):
            if isinstance(x,bytes):
                return x.decode(self.charset,self.encoding_errors)
            else:
                return str(x)
    
    else:
        def dumps(self, x):
            if not isinstance(x,unicode):
                x = str(x)
            return x.encode(self.charset,self.encoding_errors)
            
        def loads(self, x):
            if not isinstance(x,unicode):
                x = str(x)
            return x.decode(self.charset,self.encoding_errors)
    
    
class Bytes(object):
    '''The binary unicode encoder'''
    def __init__(self, charset = 'utf-8', encoding_errors = 'strict'):
        self.charset = charset
        self.encoding_errors = encoding_errors
        
    def dumps(self, x):
        if not isinstance(x,bytes):
            x = x.encode(self.charset,self.encoding_errors)
        return x
    
    loads = dumps


class NoEncoder(object):
    '''A dummy encoder class'''
    def dumps(self, x):
        return x
    
    def loads(self, x):
        return x
    
    
class PythonPickle(object):
    '''A safe pickle serializer.'''
    def dumps(self, x):
        if x is not None:
            try:
                return pickle.dumps(x)
            except:
                pass
    
    def loads(self,x):
        if x is None:
            return x
        elif isinstance(x, bytes):
            try:
                return pickle.loads(x)
            except pickle.UnpicklingError:
                return None
        else:
            return x
    

class Json(Default):
    
    def __init__(self,
                 charset = 'utf-8',
                 encoding_errors = 'strict',
                 json_encoder = JSONDateDecimalEncoder,
                 object_hook = DefaultJSONHook):
        super(Json,self).__init__(charset, encoding_errors)
        self.json_encoder = JSONDateDecimalEncoder
        self.object_hook = object_hook
        
    def dumps(self, x):
        s = json.dumps(x, cls=self.json_encoder)
        return s.encode(self.charset,self.encoding_errors)
    
    def loads(self, x):
        s = x.decode(self.charset,self.encoding_errors)
        return json.loads(s, object_hook=self.object_hook)

