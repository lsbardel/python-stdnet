import copy

from stdnet.exceptions import *
from stdnet.utils import zip, UnicodeMixin
from stdnet import dispatch

from .base import StdNetType


post_save = dispatch.Signal()


__all__ = ['StdModel',
           'StdNetType',
           'model_to_dict',
           'post_save']


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
        #if commit:
        #    post_save.send(instance = self)
        #return r
    
    def is_valid(self):
        return self._meta.is_valid(self)

    def _valattr(self, name):
        if hasattr(self,self._meta.VALATTR):
            return getattr(self,self._meta.VALATTR)[name]
            
    @property
    def cleaned_data(self):
        return self._valattr('data')
        
    @property
    def errors(self):
        return self._valattr('errors')
        
    @property
    def indices(self):
        return self._valattr('indices')
    
    @property
    def uuid(self):
        '''Universally unique identifier for a model instance.'''
        if not self.id:
            raise self.DoesNotExist('Object not saved. Cannot obtain universally unique id')
        return '{0}.{1}'.format(self._meta.hash,self.id)
        
    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return self.id == other.id
        else:
            return False
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        try:
            return hash(self.uuid)
        except self.DoesNotExist as e:
            raise TypeError(str(e))
        
    def delete(self, dlist = None):
        '''Delete an instance from database. If the instance is not available (it does not have an id) and
``StdNetException`` exception will raise. Return the number of model instances deleted.'''
        meta = self._meta
        if not self.id:
            raise StdNetException('Cannot delete object. It was never saved.')
        T = 0
        # Gather related objects to delete
        objs = self.related_objects()
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
        
    def afterload(self):
        pass
    
    # UTILITY METHODS
    
    def instance_keys(self):
        '''Utility method for returning keys associated with this instance only. The instance id
is however available in other keys (indices and other backend containers).'''
        return self._meta.cursor.instance_keys(self)
            
    @classmethod
    def commit(cls):
        '''Shortcut to commit changes'''
        return cls._meta.cursor.commit()
    
    @classmethod
    def flush(cls, count = None):
        '''Flush the model table and all related tables including all indexes.
Calling flush will erase everything about the model instances in the remote server.
If count is a dictionary, the method
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
                
