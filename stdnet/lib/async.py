try:
    from pulsar import is_async, Deferred
except ImportError:
    def is_async(result):
        return False
    
    class Deferred(object):
        
        def callback(self, result):
            self.result = result
            return self
    
    
__all__ = ['is_async', 'on_result', 'on_error', 'Deferred']


def on_result(result, callback):
    if is_async(result):
        return result.add_callback(callback)
    else:
        return callback(result)
    
def on_error(result, callback):
    if is_async(result):
        return result.add_errback(callback)
    else:
        return result