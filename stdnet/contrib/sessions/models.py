import base64
import cPickle as pickle

from stdnet import orm
from django.utils.hashcompat import md5_constructor

session_settings = {'SECRET_KEY': None}

class SuspiciousOperation(Exception):
    pass


class EncodedPickledObjectField(orm.CharField):
    
    def to_python(self, value):
        encoded_data = base64.decodestring(self.data)
        pickled, tamper_check = encoded_data[:-32], encoded_data[-32:]
        if md5_constructor(pickled + session_settings['SECRET_KEY']).hexdigest() != tamper_check:
            raise SuspiciousOperation("User tampered with session cookie.")
        try:
            return pickle.loads(pickled)
        except:
            return {}
        
    def serialise(self, value):
        pickled = pickle.dumps(session_dict)
        pickled_md5 = md5_constructor(pickled + session_settings['SECRET_KEY']).hexdigest()
        return base64.encodestring(pickled + pickled_md5)


class Session(orm.StdModel):
    id     = orm.SymbolField(primary_key=True)
    data   = orm.PickleObjectField()
    expiry = orm.DateTimeField(index = False, required = False)
