'''A micro-asyncronous script derived from twisted.'''
import sys
import traceback
from collections import deque


__all__ = ['Deferred', 'MultiDeferred']

    
def iterdata(stream):
    if isinstance(stream, dict):
        return stream.items()
    else:
        return enumerate(stream)

def isgenerator(value):
    return hasattr(value, '__iter__') and not hasattr(value, '__len__')


pass_through = lambda result: result


class Failure(object):
    '''An exception during :class:`Deferred` callbacks.
    
.. attribute:: exc_info
    
'''
    def __init__(self):
        self.exc_info = sys.exc_info()
            
    def __repr__(self):
        return self.format()
    __str__ = __repr__
    
    def format(self):
        tb = traceback.format_exception(*self.exc_info)
        return '\n'.join(tb)
    
    def raise_error(self):
        if sys.version_info > (3,1):
            _,err,traceback = self.exc_info
            raise err.with_traceback(traceback)
        else:   # pragma nocover
            raise
    
    
class Deferred(object):
    """This is a callback which will be put off until later.
The idea is the same as the ``twisted.defer.Deferred`` object.

Use this class to return from functions which otherwise would block the
program execution. Instead, it should return a Deferred.

.. attribute:: called

    ``True`` if the deferred was called. In this case the asynchronous result
    is ready and available in the attr:`result`.
    
"""
    paused = 0
    _called = False
    _runningCallbacks = False
    def __init__(self):
        self._callbacks = deque()
    
    def __repr__(self):
        v = self.__class__.__name__
        if self.called:
            v += ' ({0})'.format(self.result)
        return v
    
    def __str__(self):
        return self. __repr__()            
    
    @property
    def called(self):
        return self._called
    
    @property
    def running(self):
        return self._runningCallbacks
    
    def pause(self):
        """Stop processing until :meth:`unpause` is called.
        """
        self.paused += 1

    def unpause(self):
        """Process all callbacks made since :meth:`pause` was called.
        """
        self.paused -= 1
        self._run_callbacks()
    
    def add_callback(self, callback, errback = None):
        """Add a callback as a callable function.
The function takes at most one argument, the result passed to the
:meth:`callback` method."""
        errback = errback if errback is not None else pass_through
        if hasattr(callback,'__call__') and hasattr(errback,'__call__'):
            self._callbacks.append((callback, errback))
            self._run_callbacks()
        else:
            raise TypeError('callback must be callable')
        return self
    
    def add_errback(self, errback):
        self.add_callback(pass_through, errback)
        
    def _run_callbacks(self):
        if not self.called or self._runningCallbacks:
            return
        
        if self.paused:
            return
                    
        while self._callbacks:
            callbacks = self._callbacks.popleft()
            callback = callbacks[isinstance(self.result,Failure)]
            try:
                self._runningCallbacks = True
                try:
                    self.result = callback(self.result)
                finally:
                    self._runningCallbacks = False
            except:
                if not isinstance(self.result, Failure):
                    self.result = Failure()
            else:
                if isinstance(self.result, Deferred):
                    # Add a pause
                    self.pause()
                    # Add a callback to the result to resume callbacks
                    self.result.add_callback(self._continue)
                    return
    
    def add_callback_args(self, callback, *args, **kwargs):
        return self.add_callback(\
                lambda result : callback(result,*args,**kwargs))
        
    def _continue(self, result):
        self.result = result
        self.unpause()
        return self.result
    
    def callback(self, result = None):
        '''Run registered callbacks with the given *result*.
This can only be run once. Later calls to this will raise
:class:`AlreadyCalledError`. If further callbacks are added after
this point, :meth:`add_callback` will run the *callbacks* immediately.

:return: the *result* input parameter
'''
        if isinstance(result, Deferred):
            raise RuntimeError('Received a deferred instance from '
                               'callback function')
        if self.called:
            raise RuntimeError('Deferred {0} already called'.format(self))
        self.result = result
        self._called = True
        self._run_callbacks()
        return self.result
        
            
class MultiDeferred(Deferred):
    
    def __init__(self, type = list):
        self._locked = False
        self._deferred = {}
        self._stream = type()
        super(MultiDeferred, self).__init__()
        
    def lock(self):
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                        ' cannot be locked twice.')
        self._locked = True
        if not self._deferred:
            self._finish()
        return self
    
    def update(self, stream):
        add = self._add
        for key, value in iterdata(stream):
            add(key, value)
        
    def _add(self, key, value):
        if self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot add a dependent once locked.')
        if isinstance(value, Deferred):
            if value.called:
                value = value.result
            else:
                self._add_deferred(key, value)
        else:
            if isgenerator(value):
                value = list(value)
            if isinstance(value, (dict,list,tuple,set,frozenset)):
                if isinstance(value,dict):
                    md = MultiDeferred(type=dict)
                else:
                    md = MultiDeferred()
                md.update(value)
                md.lock()
                value = md
                if value.called:
                    value = value.result
                else:
                    self._add_deferred(key, value)
        self._setitem(key, value)
                    
    def _add_deferred(self, key, value):
        self._deferred[key] = value
        value.add_callback_args(self._deferred_done, key)
        
    def _deferred_done(self, result, key):
        self._deferred.pop(key, None)
        self._setitem(key, result)
        if self._locked and not self._deferred and not self.called:
            self._finish()
        return result
    
    def _finish(self):
        if not self._locked:
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot finish until completed.')
        if self._deferred:
            raise RuntimeError(self.__class__.__name__ +\
                               ' cannot finish whilst waiting for '
                               'dependents %r' % self._deferred)
        if self.called:
            raise RuntimeError(self.__class__.__name__ +\
                               ' done before finishing.')
        self.callback(self._stream)
        
    def _setitem(self, key, value):
        stream = self._stream
        if isinstance(stream, list) and key == len(stream):
            stream.append(value)
        else:
            stream[key] = value
     
        
    