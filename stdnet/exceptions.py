
class StdNetException(Exception):
    '''A general StdNet exception'''
    pass

class ModelNotRegistered(StdNetException):
    '''A :class:`StdNetException` raised when trying to save an instance of a :class:`stdnet.orm.StdModel` not yet
registered with a :class:`stdnet.backends.BackendDataServer`. Check :func:`stdnet.orm.register` for details.'''
    pass

class ObjectNotValidated(StdNetException):
    '''A :class:`StdNetException` raised when an instance of a :class:`stdnet.orm.StdModel` fails to validate
(probably required :class:`stdnet.orm.Field` are missing from the instance).'''
    pass

class ImproperlyConfigured(StdNetException):
    "stdnet is somehow improperly configured"
    pass

class BadCacheDataStructure(StdNetException):
    pass

class FieldError(StdNetException):
    '''Generic Field error'''
    pass

class MultiFieldError(StdNetException):
    '''A :class:`FieldError` for :class:stdnet.orm.MultiField`.'''
    pass

class FieldValueError(FieldError):
    '''Raised when passing a wrong value to a field method'''
    pass

class QuerySetError(StdNetException):
    '''A :class:`StdNetException` raised during a :class:`stdnet.orm.query.QuerySet`
evaluation.'''
    pass

class ObjectNotFound(QuerySetError):
    '''A :class:`QuerySetError` raised when an object is not found.'''
    pass