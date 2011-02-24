import copy

from stdnet.exceptions import *
from stdnet.utils import zip, UnicodeMixin

from .base import StdNetType


__all__ = ['StdModel',
           'StdNetType',
           'model_to_dict']


StdNetBase = StdNetType('StdNetBase',(UnicodeMixin,),{})


class StdModel(StdNetBase):
    '''A model is the single, definitive source of data
about your data. It contains the essential fields and behaviors
of the data you're storing. Each model class
maps to a single :class:`stdnet.HashTable` structure via
the :attr:`StdModel._meta` attribute.

.. attribute:: _meta

    Instance of :class:`stdnet.orm.base.Metaclass`
    
'''
    is_base_class = True
    DoesNotExist = ObjectNotFound
    
    def __init__(self, **kwargs):
        for field in self._meta.scalarfields:
            name = field.name
            value = kwargs.pop(name,None)
            if value is None:
                value = field.get_default()
            setattr(self,name,value)
        setattr(self,'id',kwargs.pop('id',None))
        if kwargs:
            raise ValueError("'%s' is an invalid keyword argument for %s" % (kwargs.keys()[0],self._meta))
        #for field in self._meta.multifields:
        #    setattr(self,field.attname,field.to_python(self))
    
    def save(self, commit = True):
        '''Save the instance in the remote :class:`stdnet.HashTable`
The model must be registered with a :class:`stdnet.backends.BackendDataServer`
otherwise a :class:`stdnet.exceptions.ModelNotRegistered` exception will raise.'''
        meta = self._meta
        if not meta.cursor:
            raise ModelNotRegistered("Model '{0}' is not registered with a\
 backend database. Cannot save instance.".format(meta))
        return meta.cursor.save_object(self, commit)
    
    def isvalid(self):
        return self.meta.isvalid()
        
    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return str(self.id) == str(other.id)
        else:
            return False
        
    def delete(self, dlist = None):
        '''Delete an instance from database. If the instance is not available (it does not have an id) and
``StdNetException`` exception will raise.'''
        if dlist is None:
            dlist = []
        meta = self._meta
        if not self.id:
            raise StdNetException('Cannot delete object. It was never saved.')
        # Gather related objects to delete
        objs = self.related_objects()
        T = 0
        for obj in objs:
            T += obj.delete(dlist)
        return T + meta.cursor.delete_object(self, dlist)
    
    def related_objects(self):
        '''Collect or related objects'''
        objs = []
        for rel in self._meta.related:
            rmanager = getattr(self,rel)
            objs.extend(rmanager.all())
        return objs
    
    def todict(self):
        odict = {}
        for field in self._meta.scalarfields:
            value = getattr(self,field.attname,None)
            value = field.serialize(value)
            if value:
                odict[field.name] = value
        return odict
    
    def model_to_dict(self, fields = None, exclude = None):
        '''Convert ``self`` to a dictionary.'''
        odict = self.__dict__.copy()
        return odict
        
    def afterload(self):
        pass
    
    @classmethod
    def commit(cls):
        return cls._meta.cursor.commit()
    
    @classmethod
    def flush(cls, count = None):
        '''Flush the table and all related tables. If count is a dictionary, the method
will enumerate the number of object to delete. without deleting them.'''
        return cls._meta.flush(count)
    


def model_to_dict(instance, fields = None, exclude = None):
    if isinstance(instance,StdModel):
        return instance.todict()
    else:
        d = {}
        for field in instance._meta.fields:
            default = field.get_default()
            if default:
                d[field.name] = default
        return d
                
