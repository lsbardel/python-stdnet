from django.db.models import signals

def remove_linked(sender, instance, **kwargs):
    id = instance.id
    linked = getattr(sender._meta,'linked',None)
    if linked:
        try:
            linked.objects.get(id = id).delete()
        except:
            pass

def link_models(model1,model2):
    '''Link a django model with a stdnet model together'''
    if model1 is not model2:
        linked1 = getattr(model1._meta,'linked',None)
        linked2 = getattr(model2._meta,'linked',None)
        if not linked1 and not linked2:
            model1._meta.linked = model2
            model2._meta.linked = model1
    
    signals.pre_delete.connect(remove_linked, sender=model1)
            
    
