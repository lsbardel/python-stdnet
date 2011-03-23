import hashlib

from stdnet.utils import to_bytestring

def get_model_from_hash(hash):
    if hash in _model_dict:
        return _model_dict[hash]
    

def hashmodel(model):
    '''Calculate the Hash id of metaclass ``meta``'''
    meta = model._meta
    sha = hashlib.sha1(to_bytestring('python-stdnet({0})'.format(meta)))
    hash = sha.hexdigest()[:8]
    meta.hash = hash
    _model_dict[hash] = model


_model_dict = {}