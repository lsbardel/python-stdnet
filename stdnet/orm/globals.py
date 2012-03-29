import hashlib

from stdnet.utils import to_bytes, JSPLITTER

__all__ = ['get_model_from_hash', 'hashmodel', 'JSPLITTER']

_model_dict = {}

def get_model_from_hash(hash):
    if hash in _model_dict:
        return _model_dict[hash]
    

def hashmodel(model, library = None):
    '''Calculate the Hash id of metaclass ``meta``'''
    library = library or 'python-stdnet'
    meta = model._meta
    sha = hashlib.sha1(to_bytes('{0}({1})'.format(library,meta)))
    hash = sha.hexdigest()[:8]
    meta.hash = hash
    if hash in _model_dict:
        raise KeyError('Model "{0}" already in hash table.\
 Rename your model or the module containing the model.'.format(meta)) 
    _model_dict[hash] = model
    