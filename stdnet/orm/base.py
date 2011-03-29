import sys
import copy

from stdnet.utils import zip, to_bytestring
from stdnet.orm import signals
from stdnet.exceptions import *

from .globals import hashmodel
from .query import UnregisteredManager
from .fields import Field, AutoField


def get_fields(bases, attrs):
    fields = {}
    for base in bases:
        if hasattr(base, '_meta'):
            fields.update(copy.deepcopy(base._meta.dfields))
    
    for name,field in list(attrs.items()):
        if isinstance(field,Field):
            fields[name] = attrs.pop(name)
    
    return fields


class Metaclass(object):
    '''Utility class used for storing all information
which maps a :class:`stdnet.orm.StdModel` model into an object in the
in the remote :class:`stdnet.BackendDataServer`.
An instance is initiated when :class:`stdnet.orm.StdModel` class is created:

.. attribute:: model

    a subclass of :class:`stdnet.orm.StdModel`.
    
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
    def __init__(self, model, fields,
                 abstract = False, keyprefix = None,
                 app_label = '', verbose_name = None, **kwargs):
        self.abstract = abstract
        self.keyprefix = keyprefix
        self.model = model
        self.app_label = app_label
        self.name = model.__name__.lower()
        self.fields = []
        self.scalarfields = []
        self.multifields = []
        self.dfields = {}
        self.timeout = 0
        self.related = {}
        self.verbose_name = verbose_name or self.name
        self.maker = lambda : model.__new__(model)
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
            
        self.cursor = None
        self.keys  = None
        
    def __repr__(self):
        if self.app_label:
            return '%s.%s' % (self.app_label,self.name)
        else:
            return self.name
    
    def __str__(self):
        return self.__repr__()
        
    def basekey(self, *args):
        """Calculate the key to access model hash-table/s,
and model filters in the database.
The key is an encoded binary string. For example::
        
    >>> from examples.models import User
    >>> from orm import register
    >>> register(User)
    'redis db 7 on 127.0.0.1:6379'
    >>> User._meta.basekey()
    b'stdnet.examples.user'
    >>> a = Author(name = 'Dante Alighieri').save()
    >>> a.meta.basekey()
    b'stdnet.someappname.author'
    """
        key = '%s%s' % (self.keyprefix,self)
        for arg in args:
            key = '%s:%s' % (key,arg)
        return to_bytestring(key)
    
    def autoid(self):
        '''The id for autoincrements ids'''
        return self.basekey('ids')
    
    def is_valid(self, instance):
        '''Perform validation and stored serialized data, indexes and errors.
Return ``True`` is the instance is ready to be saved to database.'''
        v = {}
        setattr(instance,self.VALATTR,v)
        data = v['data'] = {}
        indexes = v['indices'] = []
        errors = v['errors'] = {}
        #Loop over scalar fields first
        for field in self.scalarfields:
            name = field.attname
            svalue = field.serialize(getattr(instance,name,None))
            if (svalue is None or svalue is '') and field.required:
                errors[name] = "Field '{0}' is required for '{1}'.".format(name,self)
            else:
                data[name] = svalue
                if field.index:
                    indexes.append((field,svalue))
        return len(errors) == 0
                
    def table(self):
        '''Return an instance of :class:`stdnet.HashTable` holding
the model table'''
        if not self.cursor:
            raise ModelNotRegistered('%s not registered. Call orm.register(model_class) to solve the problem.' % self)
        return self.cursor.hash(self.basekey(),self.timeout)
    
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
    

def meta_options(abstract = False,
                 keyprefix = None,
                 app_label = None,
                 **kwargs):
    return {'abstract': abstract,
            'keyprefix': keyprefix,
            'app_label':app_label}
    

