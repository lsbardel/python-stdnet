import stdnet
from stdnet.orm import signals

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
    '''Adapted from django. Adds a lookup on ``cls`` when a related field is defined using a string.'''
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
    """Handle any pending relations to the sending model. Sent from class_prepared.
    """
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

        cache_name = self.field.get_cache_name()
        try:
            return getattr(instance, cache_name)
        except AttributeError:
            val = getattr(instance, self.field.attname)
            if val is None:
                return None
            rel_obj = self.field.rel.get_related_object(val)
            setattr(instance, cache_name, rel_obj)
            return rel_obj

    def __set__(self, instance, value):
        if instance is None:
            raise AttributeError("%s must be accessed via instance" % self._field.name)
        field = self.field

        if value is None and field.required:
            raise ValueError('Cannot assign None: "%s" does not allow null values.' % field)
        elif value is not None and not isinstance(value, field.relmodel):
            raise ValueError('Cannot assign "%r": "%s" must be a "%s" instance.' %
                                (value, field, field.relmodel._meta.name))

        # If we're setting the value of a OneToOneField to None, we need to clear
        # out the cache on any old related object. Otherwise, deleting the
        # previously-related object will also cause this object to be deleted,
        # which is wrong.
        if value is None:
            # Look up the previously-related object, which may still be available
            # since we've not yet cleared out the related field.
            # Use the cache directly, instead of the accessor; if we haven't
            # populated the cache, then we don't care - we're only accessing
            # the object to invalidate the accessor cache, so there's no
            # need to populate the cache just to expire it again.
            related = getattr(instance, self.field.get_cache_name(), None)

            # If we've got an old related object, we need to clear out its
            # cache. This cache also might not exist if the related object
            # hasn't been accessed yet.
            if related:
                cache_name = self.field.related.get_cache_name()
                try:
                    delattr(related, cache_name)
                except AttributeError:
                    pass

        # Set the value of the related field
        try:
            val = value.id
        except AttributeError:
            val = None
        setattr(instance, self.field.attname, val)

        setattr(instance, self.field.get_cache_name(), value)


def _register_related(field, related):
    field.relmodel = related
    meta  = related._meta
    related_name = field.related_name or '%s_set' % field.model._meta.name
    if related_name not in meta.related and related_name not in meta.dfields:
        field.related_name = related_name
        manager = field.relmanager(related,field.model,field.name)
        setattr(related,field.related_name,manager)
        meta.related[related_name] = manager
        field.rel = manager
        return manager
    else:
        raise stdnet.FieldError("Duplicated related name %s in model %s and field %s" % (related_name,related,name))


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
        self.relmodel     = model
        self.related_name = related_name
        self.relmanager   = relmanager
    
    def register_with_related_model(self):
        add_lazy_relation(self,self.relmodel,_register_related)
        

        