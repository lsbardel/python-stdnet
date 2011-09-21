from inspect import isgenerator

from stdnet.exceptions import InvalidTransaction, ModelNotRegistered


all__ = ['transaction','attr_local_transaction','Transaction']


default_callback = lambda x : x

attr_local_transaction = '_local_transaction'


def transaction(*models, **kwargs):
    '''Create a transaction'''
    if not models:
        raise ValueError('Cannot create transaction with no models')
    cursor = None
    tra = None
    for model in models:
        c = model._meta.cursor
        if not c:
            raise ModelNotRegistered("Model '{0}' is not registered with a\
 backend database. Cannot start a transaction.".format(model))
        if cursor and cursor != c:
            raise InvalidTransaction("Models {0} are registered\
 with a different databases. Cannot create transaction"\
            .format(', '.join(('{0}'.format(m) for m in models))))
        cursor = c
        # Check for local transactions
        if hasattr(model,attr_local_transaction):
            t = getattr(model,attr_local_transaction)
            if tra:
                tra.merge(t)
            else:
                tra = t
    return tra or cursor.transaction(**kwargs)


class Transaction(object):
    '''Transaction class for pipelining commands to the back-end server.
    '''
    default_name = 'transaction'
    
    def __init__(self, server, cursor, name = None):
        self.name = name or self.default_name
        self.server = server
        self.cursor = cursor
        self._cachepipes = {}
        self._callbacks = []
    
    def add(self, func, args, kwargs, callback = None):
        '''Add an operation to the transaction. This is how operation
are added to a transaction.

:parameter func: function to call which tackes the cursor as first argument
:parameter args: tuple or varing arguments
:parameter kwargs: dictionary or key-values arguments
:parameter callback: optional callback function with arity 1.'''
        res = func(self.cursor,*args,**kwargs)
        callback = callback or default_callback
        self._callbacks.append(callback)
        
    def merge(self, other):
        '''Merge two transaction together'''
        if self.server == other.server:
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
        '''Close the transaction and commit commands to the server.'''
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
    