from inspect import isgenerator

from stdnet.exceptions import InvalidTransaction, ModelNotRegistered
from stdnet.utils import itervalues


all__ = ['transaction','attr_local_transaction','Transaction']


default_callback = lambda x : x

attr_local_transaction = '_local_transaction'


def transaction(*models, **kwargs):
    '''Create a transaction'''
    if not models:
        raise ValueError('Cannot create transaction with no models')
    backend = None
    tra = None
    for model in models:
        b = model.objects.backend
        if backend and backend != b:
            raise InvalidTransaction("Models {0} are registered\
 with a different backend databse. Cannot create transaction"\
            .format(', '.join(('{0}'.format(m) for m in models))))
        backend = b
        # Check for local transactions
        if hasattr(model,attr_local_transaction):
            t = getattr(model,attr_local_transaction)
            if tra:
                tra.merge(t)
            else:
                tra = t
    return tra or backend.transaction(**kwargs)


class Transaction(object):
    '''Transaction class for pipelining commands to the back-end backend.
An instance of this class is usally obtained by using the high level
:func:`stdnet.transaction` function.

.. attribute:: name

    Transaction name
    
.. attribute:: backend

    Instance of a :class:`stdnet.BackendDataServer` to which the transaction
    is being performed.
    
.. attribute:: cursor

    The backend cursor which manages the transaction.
    
    '''
    default_name = 'transaction'
    
    def __init__(self, backend, cursor, name = None):
        self.name = name or self.default_name
        self.backend = backend
        self.cursor = cursor
        self._cachepipes = {}
        self._callbacks = []
    
    def add(self, func, args, kwargs, callback = None):
        '''Add an new operation to the transaction.

:parameter func: function to call which accept :attr:`stdnet.Transaction.cursor`
    as its first argument.
:parameter args: tuple or varying arguments to pass to *func*.
:parameter kwargs: dictionary or key-values arguments to pass to *func*.
:parameter callback: optional callback function with arity 1 which process
    the result wonce back from the backend.'''
        res = func(self.cursor,*args,**kwargs)
        callback = callback or default_callback
        self._callbacks.append(callback)
        
    def merge(self, other):
        '''Merge two transaction together'''
        if self.backend == other.backend:
            if not other.empty():
                raise NotImplementedError()
        else:
            raise InvalidTransaction('Cannot merge transactions.')
        
    def empty(self):
        '''Check if the transaction contains query data or not.
If there is no query data the transaction does not perform any
operation in the database.'''
        for c in itervalues(self._cachepipes):
            if c.pipe:
                return False
        return self.emptypipe()
                
    def emptypipe(self):
        raise NotImplementederror
            
    def structure_pipe(self, structure):
        '''Create a pipeline for a structured datafield'''
        id = structure.id
        if id not in self._cachepipes:
            self._cachepipes[id] = structure.struct()
        return self._cachepipes[id].pipe
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()
        else:
            self._result = value
            
    def commit(self):
        '''Close the transaction and commit commands to the backend.'''
        results = self._execute() or ()
        callbacks = self._callbacks
        self._callbacks = []
        if len(results) and len(callbacks):
            self._results = ((cb(r) for cb,r in\
                                   zip(callbacks,results)))
        else:
            self._results = results
            
    def get_result(self):
        '''Retrieve the result after the transaction has executed.
This can be done once only.'''
        if not hasattr(self,'_results'):
            raise InvalidTransaction(\
                        'Transaction {0} has not been executed'.format(self))
        results = self.__dict__.pop('_results')
        if isgenerator(results):
            results = list(results)
        return results

    # VIRTUAL FUNCTIONS
    
    def _execute(self):
        raise NotImplementedError
    