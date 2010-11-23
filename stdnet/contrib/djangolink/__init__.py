from stdnet import orm
from stdnet import ObjectNotFound
from stdnet.orm.query import Manager


def remove_linked(sender, instance, **kwargs):
    linked = getattr(sender._meta,'linked',None)
    if linked:
        try:
            linked.objects._get(id = instance.id).delete()
        except ObjectNotFound:
            pass


def post_save(sender, instance, **kwargs):
    linked = getattr(sender._meta,'linked',None)
    if linked:
        id = instance.id
        try:
            cobj = linked.objects._get(id = id)
        except ObjectNotFound:
            cobj = linked(id = id)
        cobj.djobject = instance
        cobj.save()
   

class LinkedManager(Manager):
    
    def __init__(self,djmodel,model):
        self.djmodel = djmodel
        self._setmodel(model)
        self.dj      = djmodel.objects
        
    def _get(self, **kwargs):
        return super(LinkedManager,self).get(**kwargs)
        
    def get(self, **kwargs):
        try:
            return self._get(**kwargs)
        except self.model.ObjectNotFound:
            return self.djmodel.objects.get(**kwargs)
            
    def sync(self):
        all = self.all()
        for obj in self.dj.all():
            if not id in all: 
                pass


def get_djobject(self):
    obj = getattr(self,'djobject',None)
    if not obj and self.id:
        try:
            obj = self._meta.linked.objects.get(id = self.id)
            self.djobject = obj
            self.save()
        except:
            obj = None
            self.djobject = obj
    return obj

def get_djfield(self, name):
    obj = self.get_djobject()
    if obj:
        attr = getattr(obj,name,None)
        if callable(attr):
            attr = attr()
        return attr
    
add_djfield = lambda name : lambda self : get_djfield(self, name)


class StdNetDjangoLink(object):
    
    def __init__(self, name):
        self.name = name
        self.cache_name = '_%s' % name
        
    def __get__(self, instance, instance_type=None):
        val = getattr(instance,self.cache_name,None)
        if not val:
            linked = getattr(instance._meta,'linked', None)
            if linked:
                val = linked.objects.get(id = instance.id)
                setattr(instance,self.cache_name,val)
            else:
                val = None
        return val
         
    
def link_models(model1,
                model2,
                field_map = [],
                pre_delete_callback = None,
                post_save_callback = None):
    '''Links a django model with a stdnet model.
    
    :keyword model1: A :class:`django.db.models.Model` class (not instance!)
    :keyword model2: A :class:`stdnet.orm.StdModel` class.
    :keyword field_map: List of 2 elements tuples for mapping django fields in model1
                        with attribute in model2. The first element is a
                        django field of model1, the second is the name of the
                        attribute added to model2.
    :keyword pre_delete_callback: function called just before an instance of model1 is deleted.
                                  If not provided, a default functions which remove the instance of
                                  ``model2`` associated with it is used.
    :keyword post_save_callback: function called just after an instance of model1 is saved.
                                 If not provided, a default functions which updated the instance of
                                 ``model2`` associated with it is used.
                                 
This function injects methods to both model1 and model2.

model2, the StdModel, will have a new :class:`stdnet.orm.PickleObjectField`
field named ``djobject``'''
    from django.db import models
    from django.db.models.base import ModelBase
    from django.db.models import signals
    
    if isinstance(model1,ModelBase) and isinstance(model2,orm.StdNetType):
        django_linked = '%s_linked' % model2._meta.name
        linked1 = getattr(model1._meta,'linked',None)
        linked2 = getattr(model2._meta,'linked',None)
        if not linked1 and not linked2:
            setattr(model1._meta,'linked',model2)
            setattr(model2._meta,'linked',model1)
            pre_delete_callback = pre_delete_callback or remove_linked
            post_save_callback  = post_save_callback or post_save
            djfield = orm.PickleObjectField()
            djfield.register_with_model('djobject',model2)
            model2.objects = LinkedManager(model1, model2)
            setattr(model2,'get_djobject',get_djobject)
            setattr(model1,django_linked,StdNetDjangoLink(django_linked))
            for field in field_map:
                setattr(model2,field,add_djfield(field))
                
            signals.pre_delete.connect(pre_delete_callback, sender=model1)
            signals.post_save.connect(post_save_callback, sender=model1)
            
            