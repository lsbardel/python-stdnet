import stdnet
from stdnet.utils import encoders

from .session import Manager
from . import signals


RECURSIVE_RELATIONSHIP_CONSTANT = 'self'

pending_lookups = {}


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


def Many2ManyThroughModel(field):
    from stdnet.orm import StdNetType, ForeignKey
    name_model = field.model._meta.name
    name_relmodel = field.relmodel._meta.name
    name = '{0}_{1}'.format(name_model,name_relmodel)
    fields = {name_model: ForeignKey(field.model),
              name_relmodel: ForeignKey(field.relmodel,
                                        related_name = field.related_name)}
    return StdNetType(name,(StdModel,),fields)


class LazyForeignKey(object):

    def __init__(self, field):
        self.field = field

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
            raise ValueError('Cannot assign "%r": "%s" must be a "%s" instance.' %
                                (value, field, field.relmodel._meta.name))
        
        cache_name = self.field.get_cache_name()
        # If we're setting the value of a OneToOneField to None, we need to clear
        # out the cache on any old related object. Otherwise, deleting the
        # previously-related object will also cause this object to be deleted,
        # which is wrong.
        if value is None:
            # Look up the previously-related object, which may still be available
            # since we've not yet cleared out the related field.
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
    def __init__(self, field, instance = None):
        self.field = field
        super(RelatedManager,self).__init__(field.model)
        self.related_instance = instance
    
        
class One2ManyRelatedManager(RelatedManager):
    '''A specialised :class:`RelatedManager` for handling one-to-many
relationships under the hood.
If a model has a :class:`ForeignKey` field, instances of
that model will have access to the related (foreign) objects
via a simple attribute of the model.'''    
    def __get__(self, instance, instance_type=None):
        return self.__class__(self.field, instance)
    
    def session(self):
        if self.related_instance:
            session = self.related_instance.session
            if session is not None:
                return session
        raise QuerySetError('Related manager can be accessed only from\
 a loaded instance of its related model.')
            
    def query(self):
        kwargs = {self.field.name: self.related_instance}
        return super(RelatedManager,self).query().filter(**kwargs)
            

class Many2ManyRelatedManager(RelatedManager):
    '''A specialized :class:`Manager` for handling
many-to-many relationships under the hood.
When a model has a :class:`ManyToManyField`, instances
of that model will have access to the related objects via a simple
attribute of the model.'''    
    def add(self, value, transaction = None):
        '''Add *value*, an instance of ``self.model``, to the set.'''
        if not isinstance(value,self.model):
            raise FieldValueError(
                        '%s is not an instance of %s' % (value,self.model))
        trans = transaction or value.local_transaction()
        # Get the related manager
        related = getattr(value, self.to_name)
        self.st.add(value, transaction = trans)
        related.st.add(self.related_instance, transaction = trans)
        # If not part of a wider transaction, commit changes
        if not transaction:
            trans.commit()
    
    def remove(self, value, transaction = None):
        '''Remove *value*, an instance of ``self.model`` from the set of
elements contained by the field.'''
        if not isinstance(value,self.model):
            raise FieldValueError(
                        '%s is not an instance of %s' % (value,self.to._meta))
        trans = transaction or value.local_transaction()
        related = getattr(value, self.to_name)
        self.st.discard(value, transaction = trans)
        related.st.discard(self.related_instance, transaction = trans)
        # If not part of a wider transaction, commit changes
        if not transaction:
            trans.commit()
        
    def filter(self, **kwargs):
        '''Filter instances of related model.'''
        kwargs['filter_sets'] = [self.st.id]
        return super(M2MRelatedManager,self).filter(**kwargs)
        
    def exclude(self, **kwargs):
        return self.filter().exclude(**kwargs)
    
