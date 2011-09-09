import hashlib

from stdnet.utils import to_bytestring


JSPLITTER = '__'


_model_dict = {}

def get_model_from_hash(hash):
    if hash in _model_dict:
        return _model_dict[hash]
    

def hashmodel(model, library = None):
    '''Calculate the Hash id of metaclass ``meta``'''
    library = library or 'python-stdnet'
    meta = model._meta
    sha = hashlib.sha1(to_bytestring('{0}({1})'.format(library,meta)))
    hash = sha.hexdigest()[:8]
    meta.hash = hash
    _model_dict[hash] = model
    

def nested_json_value(instance, attname):
    '''Extract a value from a nested dictionary'''
    fields = attname.split(JSPLITTER)
    data = getattr(instance,fields[0])
    for field in fields[1:]:
        data = data[field]
    if isinstance(data,dict):
        data = data['']
    return data