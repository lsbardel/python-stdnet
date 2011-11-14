import sys
import copy
import hashlib
from collections import namedtuple
from uuid import uuid4

from stdnet.utils import zip, to_bytestring, to_string
from stdnet.orm import signals
from stdnet.exceptions import *

from .globals import hashmodel, JSPLITTER
from .query import UnregisteredManager
from .fields import Field, AutoField


def gen_unique_id():
    return str(uuid4())[:8]


def get_fields(bases, attrs):
    fields = {}
    for base in bases:
        if hasattr(base, '_meta'):
            fields.update(copy.deepcopy(base._meta.dfields))
    
    for name,field in list(attrs.items()):
        if isinstance(field,Field):
            fields[name] = attrs.pop(name)
    
    return fields


orderinginfo = namedtuple('orderinginfo','name field desc')


class Metaclass(object):
    '''Utility class used for storing all information
which maps a :class:`stdnet.orm.StdModel` model into an object in the
in the remote :class:`stdnet.BackendDataServer`.
An instance is initiated when :class:`stdnet.orm.StdModel` class is created:

.. attribute:: model

    a subclass of :class:`stdnet.orm.StdModel`.
    
.. attribute:: ordering

    Optional name of a :class:`stdnet.orm.Field` in the :attr:`model`.
    If provided, indeces will be sorted with respect the value of the field specidied.
    Check the :ref:`sorting <sorting>` documentation for more details.
    
    Default: ``None``.
    
.. attribute:: fields

    dictionary of :class:`stdnet.orm.Field` instances.
    
.. attribute:: abstract

    if ``True``, it represents an abstract model and no database elements are created.

.. attribute:: keyprefix

    prefix for the database table. By default it is given by ``settings.DEFAULT_KEYPREFIX``,
    where ``settings`` is obtained by::
    
        from dynts.conf import settings
    
.. attribute:: pk

    primary key ::class:`stdnet.orm.Field`

'''
    VALATTR = '_validation'
    searchengine = None
    
    def __init__(self, model, fields,
                 abstract = False, keyprefix = None,
                 app_label = '', verbose_name = None,
                 ordering = None, **kwargs):
        self.abstract = abstract
        self.keyprefix = keyprefix
        self.model = model
        self.app_label = app_label
        self.name = model.__name__.lower()
        self.fields = []
        self.scalarfields = []
        self.indices = []
        self.multifields = []
        self.dfields = {}
        self.timeout = 0
        self.related = {}
        self.verbose_name = verbose_name or self.name
        model._meta = self
        hashmodel(model)
        
        # Check if ID field exists
        try:
            pk = fields['id']
        except:
            # ID field not available, create one
            pk = AutoField(primary_key = True)
        pk.register_with_model('id',model)
        self.pk = pk
        if not self.pk.primary_key:
            raise FieldError("Primary key must be named id")
        
        for name,field in fields.items():
            if name == 'id':
                continue
            field.register_with_model(name,model)
            if field.primary_key:
                raise FieldError("Primary key already available %s." % name)
        
        self.ordering = None
        if ordering:
            self.ordering = self.get_sorting(ordering,ImproperlyConfigured)
        self.cursor = None
        for scalar in self.scalarfields:
            if scalar.index:
                self.indices.append(scalar)
        
    def maker(self):
        model = self.model
        m = model.__new__(model)
        m.afterload()
        return m
        
    def __repr__(self):
        if self.app_label:
            return '%s.%s' % (self.app_label,self.name)
        else:
            return self.name
    
    def __str__(self):
        return self.__repr__()
        
    def basekey(self, *args):
        """Calculate the key to access model data in
the backend server.
The key is an encoded binary string. For example::
        
    >>> from examples.models import User
    >>> from orm import register
    >>> register(User)
    'redis db 7 on 127.0.0.1:6379'
    >>> User._meta.basekey()
    b'stdnet.examples.user'
"""
        key = '%s%s' % (self.keyprefix,self)
        for arg in args:
            if arg is not None:
                key = '%s:%s' % (key,arg)
        return key
    
    def tempkey(self, name = None):
        if not name:
            name = str(uuid4())[:8]
        return self.basekey('tmp',name)
    
    def autoid(self):
        '''The id for autoincrements ids'''
        return self.basekey('ids')
    
    def is_valid(self, instance):
        '''Perform validation for *instance* and stores serialized data,
indexes and errors into local cache.
Return ``True`` if the instance is ready to be saved to database.'''
        v = {}
        setattr(instance,self.VALATTR,v)
        data = v['data'] = {}
        indexes = v['indices'] = []
        errors = v['errors'] = {}
        #Loop over scalar fields first
        for field in self.scalarfields:
            name = field.attname
            value = getattr(instance,name,None)
            if value is None:
                value = field.get_default()
                setattr(instance,name,value)
            try:
                svalue = field.serialize(value)
            except FieldValueError as e:
                errors[name] = str(e)
            else:
                if (svalue is None or svalue is '') and field.required:
                    errors[name] = "Field '{0}' is required for '{1}'."\
                                    .format(name,self)
                else:
                    if isinstance(svalue, dict):
                        data.update(svalue)
                    else:
                        if svalue is not None:
                            data[name] = svalue
                        if field.index:
                            indexes.append((field,svalue))
        return len(errors) == 0
                
    def table(self, transaction = None):
        '''Return an instance of :class:`stdnet.HashTable` holding
the model table'''
        if not self.cursor:
            raise ModelNotRegistered('%s not registered. Call orm.register(model_class) to solve the problem.' % self)
        return self.cursor.hash(self.basekey(),self.timeout,transaction=transaction)
    
    def flush(self, count = None):
        '''Fast method for clearing the whole table including related tables'''
        for rel in self.related.values():
            rmeta = rel.to._meta
            # This avoid circular reference
            if rmeta is not self:
                rmeta.flush(count)
        if self.cursor:
            self.cursor.flush(self, count)

    def database(self):
        return self.cursor

    def get_sorting(self, sortby, errorClass):
        s = None
        desc = False
        if sortby.startswith('-'):
            desc = True
            sortby = sortby[1:]
        if sortby == 'id':
            f = self.pk
            s = orderinginfo(f.name,f,desc)
        else:
            if sortby in self.dfields:
                f = self.dfields[sortby]
                return orderinginfo(f.name,f,desc)
            sortbys = sortby.split(JSPLITTER)
            s0 = sortbys[0]
            if len(sortbys) == 2 and s0 in self.dfields:
                f = self.dfields[s0]
                return orderinginfo(sortby,f,desc)
        if not s:
            raise errorClass('Cannot Order by attribute "{0}".\
 It is not a scalar field.'.format(sortby))

    def multifields_ids_todelete(self, instance):
        '''Return the list of ids of multifields belonging to *instance*
 which needs to be deleted when *instance* is deleted.'''
        gen = (field.id(instance) for field in self.multifields\
                                         if field.todelete())
        return [fid for fid in gen if fid]
    
    
class FakeMeta(object):
    pass
        
    
class FakeModelType(type):
    '''StdModel python metaclass'''
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, FakeModelType)]
        is_base_class = attrs.pop('is_base_class',False)
        new_class = super(FakeModelType, cls).__new__(cls, name, bases, attrs)
        if not parents or is_base_class:
            return new_class
        new_class._meta = FakeMeta()
        hashmodel(new_class)
        return new_class  


class StdNetType(type):
    '''StdModel python metaclass'''
    def __new__(cls, name, bases, attrs):
        super_new = super(StdNetType, cls).__new__
        parents = [b for b in bases if isinstance(b, StdNetType)]
        if not parents or attrs.pop('is_base_class',False):
            return super_new(cls, name, bases, attrs)
        
        # remove the Meta class if present
        meta      = attrs.pop('Meta', None)
        if meta:
            kwargs   = meta_options(**meta.__dict__)
        else:
            kwargs   = meta_options()
        
        #if kwargs['abstract']:
        #    return super_new(cls, name, bases, attrs)
        
        # remove and build field list
        fields    = get_fields(bases, attrs)        
        # create the new class
        objects   = attrs.pop('objects',None)
        new_class = super_new(cls, name, bases, attrs)
        new_class.objects = objects
        app_label = kwargs.pop('app_label')
        
        if app_label is None:
            model_module = sys.modules[new_class.__module__]
            try:
                app_label = model_module.__name__.split('.')[-2]
            except:
                app_label = ''
        
        meta = Metaclass(new_class,fields,app_label=app_label,**kwargs)
        if objects is None:
            new_class.objects = UnregisteredManager(new_class)
        signals.class_prepared.send(sender=new_class)
        return new_class
    
    def __str__(cls):
        return str(cls._meta)
    

def meta_options(abstract = False,
                 keyprefix = None,
                 app_label = None,
                 ordering = None,
                 **kwargs):
    return {'abstract': abstract,
            'keyprefix': keyprefix,
            'app_label':app_label,
            'ordering':ordering}
    

