from stdnet.exceptions import InvalidTransaction, ModelNotRegistered

all__ = ['transaction','attr_local_transaction']


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