
class StdNetException(Exception):
    '''A general StdNet exception'''
    pass


class ConnectionError(StdNetException):
    pass


class SessionNotAvailable(StdNetException):
    pass


class ModelNotAvailable(StdNetException):
    pass


class ModelNotRegistered(SessionNotAvailable):
    '''A :class:`StdNetException` raised when trying to save an instance of a :class:`stdnet.odm.StdModel` not yet
registered with a :class:`stdnet.backends.BackendDataServer`. Check :func:`stdnet.odm.register` for details.'''
    pass


class InvalidTransaction(StdNetException):
    '''A :class:`StdNetException` raised when trying to create a transaction
with models registered with different backends.'''
    pass


class ResponseError(StdNetException):
    '''Raised when an invalid response is returned from the backebd server.'''
    pass
    
    
class CommitException(ResponseError):
    '''A :class:`StdNetException` raised when trying to create a transaction
with models registered with different backends.'''
    def __init__(self, msg, failures = 1):
        self.failures = failures
        super(CommitException,self).__init__(msg)


class AlreadyRegistered(StdNetException):
    pass


class ObjectNotValidated(StdNetException):
    '''A :class:`StdNetException` raised when an instance of a :class:`stdnet.odm.StdModel` fails to validate
(probably required :class:`stdnet.odm.Field` are missing from the instance).'''
    pass


class ImproperlyConfigured(StdNetException):
    "A :class:`stdnet.StdNetException` raised when stdnet is somehow improperly configured"
    pass


class BadCacheDataStructure(StdNetException):
    pass


class FieldError(StdNetException):
    '''Generic Field error'''
    pass


class StructureFieldError(StdNetException):
    '''A :class:`stdnet.FieldError` for :class:stdnet.odm.StructureField`.'''
    pass


class FieldValueError(FieldError):
    '''A :class:`stdnet.FieldError` raised when passing a wrong
value to a field. This exception is cought during the model instance
validation algorithm in :meth:`stdnet.odm.base.Metaclass.is_valid`.'''
    pass


class QuerySetError(StdNetException):
    '''A :class:`stdnet.StdNetException` raised during a :class:`stdnet.odm.query.QuerySet`
evaluation.'''
    pass


class ObjectNotFound(QuerySetError):
    '''A :class:`QuerySetError` raised when an object is not found.'''
    pass

