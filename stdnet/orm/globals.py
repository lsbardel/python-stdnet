import hashlib

from stdnet.utils import to_bytestring


JSPLITTER = '__'


def get_model_from_hash(hash):
    if hash in _model_dict:
        return _model_dict[hash]
    

def hashmodel(model, library = None):
    '''Calculate the Hash id of metaclass ``meta``'''
    library = library or 'python-stdnet'
    if hasattr(model,'_meta'):
        meta = model._meta
    else:
        meta = model
    sha = hashlib.sha1(to_bytestring('{0}({1})'.format(library,meta)))
    hash = sha.hexdigest()[:8]
    meta.hash = hash
    _model_dict[hash] = model


_model_dict = {}