'''Asynchronous binding for stdnet.'''
from inspect import isgeneratorfunction
try:
    from pulsar import is_async, async, multi_async, maybe_async, is_failure
except ImportError:
    def is_async(result):
        return False
    
    def is_failure(result):
        return False
    
    def multi_async(data, **kwargs):
        return data
    
    def maybe_async(data):
        return data
    
    class async:
        
        def __call__(self, f):
            assert isgeneratorfunction(f), 'async decorator only for generator functions'
            def _(*args, **kwargs):
                res = tuple(f(*args, **kwargs))
                return res[-1] if res else None
            return _
                    
    
    
__all__ = ['is_async', 'on_result', 'on_error', 'async',
           'multi_async', 'maybe_async']


def on_result(result, callback, errback=None):
    if is_async(result):
        return result.add_callback(callback, errback)
    elif is_failure(result):
        result.raise_all()
    else:
        return callback(result)
    
def on_error(result, callback):
    if is_async(result):
        return result.add_errback(callback)
    else:
        return result