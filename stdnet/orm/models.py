import copy
from itertools import izip

from base import StdNetType
from fields import _novalue
from stdnet.exceptions import *


class StdModel(object):
    '''A model is the single, definitive source of data
about your data. It contains the essential fields and behaviors
of the data you're storing. Each model class
maps to a single :class:`stdnet.HashTable` structure. via
the :attr:`StdModel._meta` attribute.

.. attribute:: _meta

    Instance of :class:`stdnet.orm.base.Metaclass`
    
'''
    __metaclass__ = StdNetType
    
    def __init__(self, **kwargs):
        for field in self._meta.fields:
            name = field.name
            value = kwargs.pop(name,None)
            if value is None:
                value = field.get_default()
            setattr(self,name,value)
        setattr(self,'id',kwargs.pop('id',None))
        if kwargs:
            raise ValueError("'%s' is an invalid keyword argument for this function" % kwargs.keys()[0])
        
    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__,self)
    
    def __str__(self):
        return ''
    
    def _load(self, kwargs):
        meta = copy.deepcopy(self.__class__._meta)
        self.__dict__['_meta'] = meta
        for name,field in meta.fields.iteritems():
            value = kwargs.pop(name,_novalue)
            self.__set_field(name, field, value)
            
        for name,value in kwargs.items():
            setattr(self,name,value)
        
        for name,related in meta.related.iteritems():
            related.obj = self
            #setattr(self,name,related)
        
    def a__setattr__(self, name, value):
        field = self._meta.fields.get(name,None)
        self.__set_field(name, field, value)
        
    def a__getattr__(self, name):
        field = self._meta.fields.get(name,None)
        if field:
            return field.get_full_value()
        else:
            try:
                return self.__dict__[name]
            except KeyError:
                try:
                    return self._meta.related[name]
                except KeyError:
                    return self.customAttribute(name)
        
    def customAttribute(self, name):
        '''Override this function to provide custom attributes'''
        raise AttributeError("object '%s' has not attribute %s" % (self,name))
    
    def __set_field(self, name, field, value):
        if field:
            field._set_value(name,self,value)
        else:
            self.__dict__[name] = value
    
    def save(self, commit = True):
        '''Save the instance in the remote :class:`stdnet.HashTable`
The model must be registered with a backend
otherwise a ``ModelNotRegistered`` exception will be raised.'''
        meta = self._meta
        if not meta.cursor:
            raise ModelNotRegistered('Model %s is not registered with a backend database. Cannot save any instance.' % meta.name)
        data = []
        indexes = []
        for field in meta.fields:
            name = field.name
            value = getattr(self,name,None)
            serializable = field.serialize(value)
            if serializable is None and field.required:
                raise FieldError('Field %s has no value for %s' % (field,self))
            data.append(serializable)
            if field.index:
                indexes.append((field,serializable))
        self.id = meta.pk.serialize(self.id)
        meta.cursor.add_object(self, data, indexes, commit = commit)
        return self
    
    def isvalid(self):
        return self.meta.isvalid()
        
    def __getstate__(self):
        return self.todict()
    
    def __setstate__(self,dict):
        self._load(dict)
        
    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return str(self.id) == str(other.id)
        else:
            return False
        
    def delete(self):
        '''Delete an instance from database. If the instance is not available (it does not have an id) and
``StdNetException`` exception will raise.'''
        meta = self._meta
        if not self.id:
            raise StdNetException('Cannot delete object. It was never saved.')
        # Gather related objects to delete
        objs = meta.related_objects()
        T = 0
        for obj in objs:
            T += obj.delete()
        return T + meta.cursor.delete_object(self)
    
    def todict(self):
        odict = self.__dict__.copy()
        meta = odict.pop('_meta')
        for name,field in meta.fields.items():
            val = field.serialize()
            if val is not None:
                odict[name] = val
            else:
                if field.required:
                    raise ValueError("Field %s is required" % name)
                else:
                    odict.pop(name,None)
        return odict
        
    @classmethod
    def commit(cls):
        return cls._meta.cursor.commit()
    
    

