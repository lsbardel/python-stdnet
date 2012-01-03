import stdnet
from stdnet.orm import signals

from .session import Manager

pending_lookups = {}

RECURSIVE_RELATIONSHIP_CONSTANT = 'self'


class ModelFieldPickler(object):
    
    def __init__(self, model):
        self.model = model
        
    def loads(self, s):
        return self.model.objects.get(id = s)
    
    def dumps(self, obj):
        return obj.id
    

def add_lazy_relation(field, relation, operation):
    '''Adapted from django. Adds a lookup on ``cls`` when a related
field is defined using a string.'''
    # Check for recursive relations
    relmodel = None
    if relation == RECURSIVE_RELATIONSHIP_CONSTANT:
        relmodel = field.model
    else:
        try:
            app_label, model_name = relation.split(".")
        except ValueError:
            # If we can't split, assume a model in current app
            app_label = field.model._meta.app_label
            model_name = relation
        except AttributeError:
            relmodel = relation

    if relmodel:
        operation(field, relmodel)
    else:
        key = (app_label, model_name)
        value = (field, operation)
        pending_lookups.setdefault(key, []).append(value)
        
        
def do_pending_lookups(sender, **kwargs):
    """Handle any pending relations to the sending model.
Sent from class_prepared."""
    key = (sender._meta.app_label, sender.__name__)
    for field, operation in pending_lookups.pop(key, []):
        operation(field, sender)


signals.class_prepared.connect(do_pending_lookups)


class ReverseSingleRelatedObjectDescriptor(object):

    def __init__(self, field_with_rel):
        self.field = field_with_rel

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
            
            rel_obj = field.rel.get_related_object(field.relmodel,val)
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


def _register_related(field, related):
    field.relmodel = related
    meta  = related._meta
    related_name = field.related_name or '%s_set' % field.model._meta.name
    if related_name not in meta.related and related_name not in meta.dfields:
        field.related_name = related_name
        manager = field.relmanager(field.model, field.name)
        setattr(related,field.related_name,manager)
        meta.related[related_name] = manager
        field.rel = manager
        return manager
    else:
        raise stdnet.FieldError('Duplicated related name "{0}"\
 in model "{1}" and field {2}'.format(related_name,meta,field))


def _register_container_model(field, related):
    field.relmodel = related
    if not field.pickler:
        field.pickler = ModelFieldPickler(related)


class RelatedObject(object):
    
    def __init__(self,
                 model,
                 related_name = None,
                 relmanager = None):
        if not model:
            raise stdnet.FieldError('Model not specified')
        self.relmodel = model
        self.related_name = related_name
        self.relmanager = relmanager
    
    def register_with_related_model(self):
        add_lazy_relation(self, self.relmodel, _register_related)
        

class BaseRelatedManager(Manager):
    
    def __init__(self, model, instance = None):
        super(BaseRelatedManager,self).__init__(model)
        self.related_instance = instance
    
        
class RelatedManager(BaseRelatedManager):
    '''A specialized :class:`Manager` for handling
one-to-many relationships under the hood.
If a model has a :class:`stdnet.orm.ForeignKey` field, instances of
that model will have access to the related (foreign) objects
via a simple attribute of the model.'''
    def __init__(self, model, related_fieldname, instance = None):
        super(RelatedManager,self).__init__(model,instance)
        self.related_fieldname = related_fieldname
        if self.related_instance is not None:
            self.backend = model.objects.backend            
    
    def __get__(self, instance, instance_type=None):
        return self.__class__(self.model, self.related_fieldname, instance)
    
    def _get_field(self):
        return self.related_instance._meta.dfields[self.fieldname]
    field = property(_get_field)
    
    def get_related_object(self, model, id):
        return model.objects.get(id = id)
        
    def filter(self, **kwargs):
        if self.related_instance:
            kwargs[self.related_fieldname] = self.related_instance
            return super(RelatedManager,self).filter(**kwargs)
        else:
            raise QuerySetError('Related manager can be accessed only from\
 an instance of its related model.')
            
    def exclude(self, **kwargs):
        return self.filter().exclude(**kwargs)
        

class M2MRelatedManager(BaseRelatedManager):
    '''A specialized :class:`Manager` for handling
many-to-many relationships under the hood.
When a model has a :class:`ManyToManyField`, instances
of that model will have access to the related objects via a simple
attribute of the model.'''
    def __init__(self, model, st, to_name, instance):
        super(M2MRelatedManager,self).__init__(model, instance)
        self.st = st
        self.to_name = to_name
    
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
    
