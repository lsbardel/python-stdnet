try:
    from pulsar import is_async
except ImportError:
    def is_async(result):
        return False
    
__all__ = ['is_async', 'on_result']


def on_result(result, callback, *args, **kwargs):
    if is_async(result):
        return result.add_callback(lambda res : callback(res, *args, **kwargs))
    else:
        return callback(result, *args, **kwargs)