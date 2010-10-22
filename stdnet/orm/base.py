import sys
import copy
from itertools import izip
from fields import Field, AutoField
from stdnet.exceptions import *
from query import UnregisteredManager 

def get_fields(bases, attrs):
    fields = {}
    for base in bases:
        if hasattr(base, '_meta'):
            fields.update(copy.deepcopy(base._meta.fields))
    
    for name,field in attrs.items():
        if isinstance(field,Field):
            fields[name] = attrs.pop(name)
    
    return fields



class Metaclass(object):
    '''Utility class used for storing all information
which maps a :class:`stdnet.orm.StdModel` model
into a :class:`stdnet.HashTable` structure in a :class:`stdnet.BackendDataServer`.
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
    def __init__(self, model, fields,
                 abstract = False, keyprefix = None,
                 app_label = None, **kwargs):
        self.abstract  = abstract
        self.keyprefix = keyprefix
        self.model     = model
        self.app_label = app_label
        self.name      = model.__name__.lower()
        self.fields    = []
        self.dfields   = {}
        self.timeout   = 0
        self.related   = {}
        self.maker     = lambda : model.__new__(model)
        model._meta    = self
        
        if not abstract:
            try:
                pk = fields['id']
            except:
                pk = AutoField(primary_key = True)
            pk.register_with_model('id',model)
            self.pk = pk
            if not self.pk.primary_key:
                raise FieldError("Primary key must be named id")
            
            for name,field in fields.iteritems():
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
        '''Calculate the key to access model hash-table, and model filters in the database.
        For example::
        
            >>> a = Author(name = 'Dante Alighieri').save()
            >>> a.meta.basekey()
            'stdnet:author'
            '''
        key = '%s%s' % (self.keyprefix,self.name)
        for arg in args:
            key = '%s:%s' % (key,arg)
        return key
    
    def autoid(self):
        return self.basekey('ids')
    
    @property
    def uniqueid(self):
        '''Unique id for an instance. This is unique across multiple model types.'''
        return self.basekey(self.id)
    
    def table(self):
        '''Return an instance of :class:`stdnet.HashTable` holding
the model table'''
        if not self.cursor:
            raise ModelNotRegistered('%s not registered. Call orm.register(model_class) to solve the problem.' % self)
        return self.cursor.hash(self.basekey(),self.timeout)
    
    def related_objects(self):
        objs = []
        for rel in self.related.itervalues():
            objs.extend(rel.all())
        return objs
    
    def make(self, id, data):
        '''Create a model instance from server data'''
        obj = self.maker()
        setattr(obj,'id',id)
        for field,value in izip(self.fields,data):
            setattr(obj,field.name,field.to_python(value))
        return obj


class StdNetType(type):
    '''StdModel python metaclass'''
    def __new__(cls, name, bases, attrs):
        super_new = super(StdNetType, cls).__new__
        parents = [b for b in bases if isinstance(b, StdNetType)]
        if not parents:
            return super_new(cls, name, bases, attrs)
        
        # remove the Meta class if present
        meta      = attrs.pop('Meta', None)
        # remove and build field list
        fields    = get_fields(bases, attrs)        
        # create the new class
        new_class = super_new(cls, name, bases, attrs)
        if meta:
            kwargs   = meta_options(**meta.__dict__)
        else:
            kwargs   = {}
        meta = Metaclass(new_class,fields,**kwargs)
        if getattr(meta, 'app_label', None) is None:
            # Figure out the app_label a-la django
            model_module = sys.modules[new_class.__module__]
            setattr(meta,'app_label',model_module.__name__.split('.')[-2])
            
        objects = getattr(new_class,'objects',None)
        if objects is None:
            new_class.objects = UnregisteredManager(new_class)
        return new_class
    


def meta_options(abstract = False,
                 keyprefix = None,
                 **kwargs):
    return {'abstract': abstract,
            'keyprefix': keyprefix}
    