import stdnet
from stdnet.utils import encoders, iteritems
from stdnet import FieldValueError

from .session import Manager
from . import signals


RECURSIVE_RELATIONSHIP_CONSTANT = 'self'

pending_lookups = {}


__all__ = ['LazyForeignQuery',
           'LazyForeignKey',
           'ModelFieldPickler']


class ModelFieldPickler(encoders.Encoder):
    '''An encoder for :class:`StdModel` instances.'''
    def __init__(self, model):
        self.model = model

    def loads(self, s):
        return self.model.objects.get(id = s)

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
    '''Create a Many2Many through model with two foreign key fields'''
    from stdnet.odm import StdNetType, StdModel, ForeignKey
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
                                                    name_relmodel,
                                                    through))
    field1.register_with_model(name_model, through)
    # The second field
    field2 = ForeignKey(field.relmodel,
                        related_name=field.related_name,
                        related_manager_class=makeMany2ManyRelatedManager(
                                                    field.model,
                                                    name_relmodel,
                                                    name_model,
                                                    through))
    field2.register_with_model(name_relmodel, through)


class LazyForeignQuery(object):
    '''Descriptor for a :class:`ForeignKey` field.'''
    def __init__(self, field):
        self.field = field

    def filter(self, instance):
        val = getattr(instance, self.field.attname)
        if val is not None:
            return {self.field.relmodel._meta.pkname(): val}

    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self
        f = self.filter(instance)
        if f:
            return instance.session.query(self.field.relmodel).filter(**f)


class LazyForeignKey(LazyForeignQuery):

    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self
        field = self.field
        cache_name = field.get_cache_name()
        try:
            return getattr(instance, cache_name)
        except AttributeError:
            val = getattr(instance, field.attname)
            if val is None:
                return None

            rel_obj = instance.session.query(field.relmodel).get(id = val)
            setattr(instance, cache_name, rel_obj)
            return rel_obj

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


def _register_container_model(field, related):
    field.relmodel = related
    if not field.pickler:
        field.pickler = ModelFieldPickler(related)


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

    def session(self, transaction = None):
        '''Retrieve the session for this :class:`RelatedManager`.

:parameter transaction: an optional session :class:`Transaction` to use.
:rtype: a :class:`Session`.'''
        if transaction:
            return transaction.session
        elif self.related_instance:
            session = self.related_instance.session
            if session is not None:
                return session
        raise QuerySetError('Related manager can be accessed only from\
 a loaded instance of its related model.')


class One2ManyRelatedManager(RelatedManager):
    '''A specialised :class:`RelatedManager` for handling one-to-many
relationships under the hood.
If a model has a :class:`ForeignKey` field, instances of
that model will have access to the related (foreign) objects
via a simple attribute of the model.'''
    @property
    def relmodel(self):
        return self.field.relmodel

    def query(self, transaction = None):
        kwargs = {self.field.name: self.related_instance}
        return super(RelatedManager,self).query(transaction = transaction)\
                                         .filter(**kwargs)

    def query_from_query(self, query):
        return query.session.query(self.model, fargs={self.field.name: query})


def makeMany2ManyRelatedManager(formodel, name_relmodel,
                                name_formodel, through):
    '''formodel is the model which the manager .'''
    class Many2ManyRelatedManager(One2ManyRelatedManager):
        '''A specialized :class:`Manager` for handling
many-to-many relationships under the hood.
When a model has a :class:`ManyToManyField`, instances
of that model will have access to the related objects via a simple
attribute of the model.'''
        def session_kwargs(self, value, transaction):
            if not isinstance(value,self.formodel):
                raise FieldValueError(
                    '%s is not an instance of %s' % (value,self.formodel._meta))
            # Get the related manager
            kwargs = {self.name_formodel: value,
                      self.name_relmodel: self.related_instance}
            return self.session(transaction), kwargs

        def add(self, value, transaction=None, **kwargs):
            '''Add *value*, an instance of ``self.formodel``,
            to the throw model.'''
            session, kw = self.session_kwargs(value, transaction)
            try:
                m = session.query(self.model).get(**kw)
                if not kwargs:
                    return m
            except self.model.DoesNotExist:
                m = self.model(**kw)
            for k,v in iteritems(kwargs):
                setattr(m, k, v)
            return session.add(m)

        def remove(self, value, transaction=None):
            '''Remove *value*, an instance of ``self.model`` from the set of
    elements contained by the field.'''
            session, kwargs = self.session_kwargs(value, transaction)
            query = session.query(self.model).filter(**kwargs)
            session.delete(query)

        def throughquery(self, transaction=None):
            '''Return a query on the *throughmodel*, the model
used to hold the many-to-many relationship.'''
            return super(Many2ManyRelatedManager,self).query(
                                                    transaction=transaction)

        def query(self, transaction=None):
            ids = self.throughquery().get_field(self.name_formodel)
            session = self.session(transaction)
            return session.query(self.formodel).filter(id__in=ids)

    Many2ManyRelatedManager.formodel = formodel
    Many2ManyRelatedManager.name_relmodel = name_relmodel
    Many2ManyRelatedManager.name_formodel = name_formodel
    Many2ManyRelatedManager.through = through
    return Many2ManyRelatedManager