from django.db import models
from django.db.models.base import ModelBase
from django.db.models import signals

from stdnet import orm
from stdnet import ObjectNotFund
from stdnet.orm.query import Manager


def remove_linked(sender, instance, **kwargs):
    linked = getattr(sender._meta,'linked',None)
    if linked:
        try:
            linked.objects.get(id = id).delete()
        except ObjectNotFund:
            pass


def post_save(sender, instance, **kwargs):
    linked = getattr(sender._meta,'linked',None)
    if linked:
        id = instance.id
        try:
            cobj = linked.objects.get(id = id)
        except ObjectNotFund:
            cobj = linked(id = id)
        cobj._linked = obj
        cobj.save()
   

class LinkedManager(Manager):
    
    def __init__(self,
                 djmodel,
                 model,
                 pre_delete_callback,
                 post_save_callback):
        self.djmodel = djmodel
        self._setmodel(model)
        self.dj      = djmodel.objects
        signals.pre_delete.connect(pre_delete_callback, sender=djmodel)
        signals.post_save.connect(post_save_callback, sender=djmodel)
        
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
    
def link_models(model1,
                model2,
                field_map = [],
                pre_delete_callback = None,
                post_save_callback = None):
    '''Link a django model with a stdnet model together'''
    if isinstance(model1,ModelBase) and isinstance(model2,orm.StdNetType):
        linked1 = getattr(model1._meta,'linked',None)
        linked2 = getattr(model2._meta,'linked',None)
        if not linked1 and not linked2:
            setattr(model1._meta,'linked',model2)
            setattr(model2._meta,'linked',model1)
            pre_delete_callback = pre_delete_callback or remove_linked
            post_save_callback  = post_save_callback or post_save
            djfield = orm.PickleObjectField()
            djfield.register_with_model('djobject',model2)
            model2.objects = LinkedManager(model1,
                                           model2,
                                           pre_delete_callback,
                                           post_save_callback)
            setattr(model2,'get_djobject',get_djobject)
            for field in field_map:
                setattr(model2,field,add_djfield(field))
            
            