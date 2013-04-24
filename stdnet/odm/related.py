import stdnet
from stdnet.utils import encoders, iteritems
from stdnet import FieldValueError, QuerySetError, ManyToManyError, async

from .session import Manager
from . import signals


RECURSIVE_RELATIONSHIP_CONSTANT = 'self'

pending_lookups = {}


__all__ = ['LazyForeignKey', 'ModelFieldPickler']


class ModelFieldPickler(encoders.Encoder):
    '''An encoder for :class:`StdModel` instances.'''
    def __init__(self, model):
        self.model = model

    def loads(self, s):
        return self.model.objects.get(id=s)

    def dumps(self, obj):
        return obj.id


def load_relmodel(field, callback):
    relmodel = None
    relation = field.relmodel
    if relation == RECURSIVE_RELATIONSHIP_CONSTANT:
        relmodel = field.model
    else:
        try:
            app_label, model_name = relation.lower().split(".")
        except ValueError:
            # If we can't split, assume a model in current app
            app_label = field.model._meta.app_label
            model_name = relation.lower()
        except AttributeError:
            relmodel = relation
    if relmodel:
        callback(relmodel)
    else:
        key = (app_label, model_name)
        if key not in pending_lookups:
            pending_lookups[key] = []
        pending_lookups[key].append(callback)


def do_pending_lookups(sender, **kwargs):
    """Handle any pending relations to the sending model.
Sent from class_prepared."""
    key = (sender._meta.app_label, sender._meta.name)
    for callback in pending_lookups.pop(key, []):
        callback(sender)


signals.class_prepared.connect(do_pending_lookups)


class ProxyManager(Manager):

    def __init__(self, proxymodel):
        self.model = None
        self.proxymodel = proxymodel

    @property
    def backend(self):
        return self.proxymodel.objects.backend

    def register(self, model, backend=None):
        self.model = model


def Many2ManyThroughModel(field):
    '''Create a Many2Many through model with two foreign key fields and a
CompositeFieldId depending on the two foreign keys.'''
    from stdnet.odm import StdNetType, StdModel, ForeignKey, CompositeIdField
    name_model = field.model._meta.name
    name_relmodel = field.relmodel._meta.name
    # The two models are the same.
    if name_model == name_relmodel:
        name_relmodel += '2'
    through = field.through
    # Create the through model
    if through is None:
        name = '{0}_{1}'.format(name_model, name_relmodel)
        pmanager = ProxyManager(field.model)
        through = StdNetType(name, (StdModel,), {'objects': pmanager})
        pmanager.register(through)
        field.through = through
    # The first field
    field1 = ForeignKey(field.model,
                        related_name=field.name,
                        related_manager_class=makeMany2ManyRelatedManager(
                                                    field.relmodel,
                                                    name_model,
                                                    name_relmodel))
    field1.register_with_model(name_model, through)
    # The second field
    field2 = ForeignKey(field.relmodel,
                        related_name=field.related_name,
                        related_manager_class=makeMany2ManyRelatedManager(
                                                    field.model,
                                                    name_relmodel,
                                                    name_model))
    field2.register_with_model(name_relmodel, through)
    pk = CompositeIdField(name_model, name_relmodel)
    pk.register_with_model('id', through)


class LazyForeignKey(object):
    '''Descriptor for a :class:`ForeignKey` field.'''
    def __init__(self, field):
        self.field = field
        
    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self
        return instance._load_related_model(self.field)

    def __set__(self, instance, value):
        if instance is None:
            raise AttributeError("%s must be accessed via instance"\
                                  % self._field.name)
        field = self.field
        if value is not None and not isinstance(value, field.relmodel):
            raise ValueError(
                        'Cannot assign "%r": "%s" must be a "%s" instance.' %
                                (value, field, field.relmodel._meta.name))

        cache_name = self.field.get_cache_name()
        # If we're setting the value of a OneToOneField to None,
        # we need to clear
        # out the cache on any old related object. Otherwise, deleting the
        # previously-related object will also cause this object to be deleted,
        # which is wrong.
        if value is None:
            # Look up the previously-related object, which may still
            # be available since we've not yet cleared out the related field.
            related = getattr(instance, cache_name, None)
            if related:
                try:
                    delattr(instance, cache_name)
                except AttributeError:
                    pass
            setattr(instance, self.field.attname, None)
        else:
            setattr(instance, self.field.attname, value.id)
            setattr(instance, cache_name, value)


class RelatedManager(Manager):
    '''Base class for managers handling relationships between models.
While standard :class:`Manager` are class properties of a model,
related managers are accessed by instances to easily retrieve instances
of a related model.'''
    def __init__(self, field, model=None, instance=None):
        self.field = field
        model = model or field.model
        super(RelatedManager,self).__init__(model)
        self.related_instance = instance

    def __get__(self, instance, instance_type=None):
        return self.__class__(self.field, self.model, instance)

    def session(self, transaction=None):
        '''Retrieve the session for this :class:`RelatedManager`.

:parameter transaction: an optional session :class:`Transaction` to use.
:rtype: a :class:`Session`.'''
        if transaction:
            return transaction.session
        session = None
        if self.related_instance:
            session = self.related_instance.session
        else:
            session = self.model.objects.session()
        if session is not None:
            return session
        raise QuerySetError('Related manager can be accessed only from\
 a loaded instance of its related model.')
        
    def query(self, transaction=None):
        '''Returns a new :class:`Query` for the :attr:`RelatedManager.model`.'''
        if transaction:
            return transaction.session.query(self.model)
        else:
            return self.session().query(self.model)


class One2ManyRelatedManager(RelatedManager):
    '''A specialised :class:`RelatedManager` for handling one-to-many
relationships under the hood.
If a model has a :class:`ForeignKey` field, instances of
that model will have access to the related (foreign) objects
via a simple attribute of the model.'''
    @property
    def relmodel(self):
        return self.field.relmodel

    def query(self, transaction=None):
        # Override query method to account for related instance if available
        query = super(One2ManyRelatedManager, self).query(transaction)
        if self.related_instance is not None:
            kwargs = {self.field.name: self.related_instance}
            return query.filter(**kwargs)
        else:
            return query

    def query_from_query(self, query, params=None):
        if params is None:
            params = query
        return query.session.query(self.model, fargs={self.field.name: params})


class Many2ManyRelatedManager(One2ManyRelatedManager):
    '''A specialized :class:`Manager` for handling
many-to-many relationships under the hood.
When a model has a :class:`ManyToManyField`, instances
of that model will have access to the related objects via a simple
attribute of the model.'''
    def session_instance(self, name, value, transaction, **kwargs):
        if self.related_instance is None:
            raise ManyToManyError('Cannot use "%s" method from class' % name)
        elif not self.related_instance.pkvalue():
            raise ManyToManyError('Cannot use "%s" method on a non persistent '
                                  'instance.' % name)
        elif not isinstance(value, self.formodel):
            raise ManyToManyError(
               '%s is not an instance of %s' % (value, self.formodel._meta))
        elif not value.pkvalue():
            raise ManyToManyError('Cannot use "%s" a non persistent instance.'\
                                  % name)
        kwargs.update({self.name_formodel: value,
                       self.name_relmodel: self.related_instance})
        return self.session(transaction), self.model(**kwargs)

    def add(self, value, transaction=None, **kwargs):
        '''Add ``value``, an instance of :attr:`formodel` to the
:attr:`through` model. This method can only be accessed by an instance of the
model for which this related manager is an attribute.'''
        s, instance = self.session_instance('add', value, transaction, **kwargs)
        return s.add(instance)

    def remove(self, value, transaction=None):
        '''Remove *value*, an instance of ``self.model`` from the set of
elements contained by the field.'''
        s, instance = self.session_instance('remove', value, transaction)
        instance.state(iid=instance.pkvalue())
        return s.delete(instance)

    def throughquery(self, transaction=None):
        '''Return a :class:`Query` on the ``throughmodel``, the model
used to hold the :ref:`many-to-many relationship <many-to-many>`.'''
        return super(Many2ManyRelatedManager, self).query(
                                                transaction=transaction)

    def query(self, transaction=None):
        # Return a query for the related model
        ids = self.throughquery().get_field(self.name_formodel)
        session = self.session(transaction)
        return session.query(self.formodel).filter(id__in=ids)
    
        
def makeMany2ManyRelatedManager(formodel, name_relmodel, name_formodel):
    '''formodel is the model which the manager .'''
    class _Many2ManyRelatedManager(Many2ManyRelatedManager):
        pass
    
    _Many2ManyRelatedManager.formodel = formodel
    _Many2ManyRelatedManager.name_relmodel = name_relmodel
    _Many2ManyRelatedManager.name_formodel = name_formodel
        
    return _Many2ManyRelatedManager