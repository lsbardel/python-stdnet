from functools import partial

from stdnet.utils import encoders
from stdnet import QuerySetError, ManyToManyError

from .globals import Event
from .session import Manager, LazyProxy

__all__ = ['LazyForeignKey', 'ModelFieldPickler']


RECURSIVE_RELATIONSHIP_CONSTANT = 'self'

pending_lookups = {}

class_prepared = Event()


class ModelFieldPickler(encoders.Encoder):

    '''An encoder for :class:`StdModel` instances.'''

    def __init__(self, model):
        self.model = model

    def dumps(self, obj):
        return obj.pkvalue()

    def require_session(self):
        return True

    def load_iterable(self, iterable, session):
        ids = []
        backend = session.model(self.model).read_backend
        tpy = self.model.pk().to_python
        ids = [tpy(id, backend) for id in iterable]
        result = session.query(self.model).filter(id=ids).all()
        return backend.execute(result, partial(self._sort, ids))

    def _sort(self, ids, results):
        results = dict(((r.pkvalue(), r) for r in results))
        return [results.get(id) for id in ids]


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


def do_pending_lookups(event, sender, **kwargs):
    """Handle any pending relations to the sending model.
Sent from class_prepared."""
    key = (sender._meta.app_label, sender._meta.name)
    for callback in pending_lookups.pop(key, []):
        callback(sender)


class_prepared.bind(do_pending_lookups)


def Many2ManyThroughModel(field):
    '''Create a Many2Many through model with two foreign key fields and a
CompositeFieldId depending on the two foreign keys.'''
    from stdnet.odm import ModelType, StdModel, ForeignKey, CompositeIdField
    name_model = field.model._meta.name
    name_relmodel = field.relmodel._meta.name
    # The two models are the same.
    if name_model == name_relmodel:
        name_relmodel += '2'
    through = field.through
    # Create the through model
    if through is None:
        name = '{0}_{1}'.format(name_model, name_relmodel)

        class Meta:
            app_label = field.model._meta.app_label
        through = ModelType(name, (StdModel,), {'Meta': Meta})
        field.through = through
    # The first field
    field1 = ForeignKey(field.model,
                        related_name=field.name,
                        related_manager_class=makeMany2ManyRelatedManager(
                            field.relmodel,
                            name_model,
                            name_relmodel)
                        )
    field1.register_with_model(name_model, through)
    # The second field
    field2 = ForeignKey(field.relmodel,
                        related_name=field.related_name,
                        related_manager_class=makeMany2ManyRelatedManager(
                            field.model,
                            name_relmodel,
                            name_model)
                        )
    field2.register_with_model(name_relmodel, through)
    pk = CompositeIdField(name_model, name_relmodel)
    pk.register_with_model('id', through)


class LazyForeignKey(LazyProxy):

    '''Descriptor for a :class:`ForeignKey` field.'''

    def load(self, instance, session=None, backend=None):
        return instance._load_related_model(self.field)

    def __set__(self, instance, value):
        if instance is None:
            raise AttributeError("%s must be accessed via instance" %
                                 self._field.name)
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
            setattr(instance, self.field.attname, value.pkvalue())
            setattr(instance, cache_name, value)


class RelatedManager(Manager):

    '''Base class for managers handling relationships between models.
While standard :class:`Manager` are class properties of a model,
related managers are accessed by instances to easily retrieve instances
of a related model.

.. attribute:: relmodel

    The :class:`StdModel` this related manager relates to.

.. attribute:: related_instance

    An instance of the :attr:`relmodel`.
'''

    def __init__(self, field, model=None, instance=None):
        self.field = field
        model = model or field.model
        super(RelatedManager, self).__init__(model)
        self.related_instance = instance

    def __get__(self, instance, instance_type=None):
        return self.__class__(self.field, self.model, instance)

    def session(self, session=None):
        '''Override :meth:`Manager.session` so that this
        :class:`RelatedManager` can retrieve the session from the
        :attr:`related_instance` if available.
        '''
        if self.related_instance:
            session = self.related_instance.session
        # we have a session, we either create a new one return the same session
        if session is None:
            raise QuerySetError('Related manager can be accessed only from\
 a loaded instance of its related model.')
        return session


class One2ManyRelatedManager(RelatedManager):

    '''A specialised :class:`RelatedManager` for handling one-to-many
relationships under the hood.
If a model has a :class:`ForeignKey` field, instances of
that model will have access to the related (foreign) objects
via a simple attribute of the model.'''
    @property
    def relmodel(self):
        return self.field.relmodel

    def query(self, session=None):
        # Override query method to account for related instance if available
        query = super(One2ManyRelatedManager, self).query(session)
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

    def session_instance(self, name, value, session, **kwargs):
        if self.related_instance is None:
            raise ManyToManyError('Cannot use "%s" method from class' % name)
        elif not self.related_instance.pkvalue():
            raise ManyToManyError('Cannot use "%s" method on a non persistent '
                                  'instance.' % name)
        elif not isinstance(value, self.formodel):
            raise ManyToManyError(
                '%s is not an instance of %s' % (value, self.formodel._meta))
        elif not value.pkvalue():
            raise ManyToManyError('Cannot use "%s" a non persistent instance.'
                                  % name)
        kwargs.update({self.name_formodel: value,
                       self.name_relmodel: self.related_instance})
        return self.session(session), self.model(**kwargs)

    def add(self, value, session=None, **kwargs):
        '''Add ``value``, an instance of :attr:`formodel` to the
:attr:`through` model. This method can only be accessed by an instance of the
model for which this related manager is an attribute.'''
        s, instance = self.session_instance('add', value, session, **kwargs)
        return s.add(instance)

    def remove(self, value, session=None):
        '''Remove *value*, an instance of ``self.model`` from the set of
elements contained by the field.'''
        s, instance = self.session_instance('remove', value, session)
        # update state so that the instance does look persistent
        instance.get_state(iid=instance.pkvalue(), action='update')
        return s.delete(instance)

    def throughquery(self, session=None):
        '''Return a :class:`Query` on the ``throughmodel``, the model
used to hold the :ref:`many-to-many relationship <many-to-many>`.'''
        return super(Many2ManyRelatedManager, self).query(session)

    def query(self, session=None):
        # Return a query for the related model
        ids = self.throughquery(session).get_field(self.name_formodel)
        pkey = self.formodel.pk().name
        fargs = {pkey: ids}
        return self.session(session).query(self.formodel).filter(**fargs)


def makeMany2ManyRelatedManager(formodel, name_relmodel, name_formodel):
    '''formodel is the model which the manager .'''

    class _Many2ManyRelatedManager(Many2ManyRelatedManager):
        pass

    _Many2ManyRelatedManager.formodel = formodel
    _Many2ManyRelatedManager.name_relmodel = name_relmodel
    _Many2ManyRelatedManager.name_formodel = name_formodel
    return _Many2ManyRelatedManager
