'''Classes usied for encoding and decoding data'''
from stdnet.utils import JSONDateDecimalEncoder


class StdnetEncoder(object):
    
    def __init__(self, charset = 'utf-8', encoding_errors = 'strict'):
        self.charset = charset
        self.encoding_errors = encoding_errors
        
    def dumps(self, x):
        return x.encode(self.charset,self.encoding_errors)
    
    def loads(self, x):
        return x.decode(self.charset,self.encoding_errors)
    

class NoEncoder(StdnetEncoder):
    
    def dumps(self, x):
        return x
    
    def loads(self, x):
        return x
    
    
class PythonPickle(NoEncoder):
    
    def dumps(self, x):
        return pickle.dumps(x,2)
    
    def loads(self,x):
        return pickle.loads(x)
    

class Json(StdnetEncoder):
    
    def dumps(self, x):
        s = json.dumps(x, cls=JSONDateDecimalEncoder)
        return s.encode(self.charset,self.encoding_errors)
    
    def loads(self, x):
        s = x.decode(self.charset,self.encoding_errors)
        return json.loads(s, object_hook=DefaultJSONHook)

