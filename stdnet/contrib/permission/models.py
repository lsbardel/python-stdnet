'''Very general permission model
'''
from inspect import isclass
from stdnet import orm


class Permission(orm.StdModel):
    '''A general permission model'''
    numeric_code = orm.IntegerField()
    user_group_model = orm.ModelField()
    user_group_id = orm.SymbolField()
    model = orm.ModelField()
    object_id = orm.SymbolField(required = False)
    
    def object(self):
        if not hasattr(self,'_object'):
            if self.object_id:
                self._object = self.model.objects.get(id = self.object_id)
            else:
                self._object = None
        return self._object
    
    
class PermissionBackend(object):
    
    def has(self, request, permission_code, obj, user = None):
        if not obj:
            return None 
        if isclass(obj):
            model = obj
            obj = None
        else:
            model = obj.__class__
        p = Permissions.objects.filter(permission_model = model)
        if not p.count():
            return None
        
    
    