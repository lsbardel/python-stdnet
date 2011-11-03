import sys
import copy
import hashlib
from collections import namedtuple

from stdnet.utils import zip, to_bytestring, to_string, gen_unique_id
from stdnet.orm import signals
from stdnet.exceptions import *

from .globals import hashmodel, JSPLITTER
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


orderinginfo = namedtuple('orderinginfo','name field desc')


class Metaclass(object):
    '''Utility class used for storing all information
which maps a :class:`stdnet.orm.StdModel` model into an object in the
in the remote :class:`stdnet.BackendDataServer`.
An instance is initiated when :class:`stdnet.orm.StdModel` class is created.

To override default behaviour you can specify the ``Meta`` class as an inner
class of :class:`stdnet.orm.StdModel` in the following way::

    from datetime import datetime
    from stdnet import orm
    
    class MyModel(orm.StdModel):
        timestamp = orm.DateTimeField(default = datetime.now)
        ...
        
        class Meta:
            ordering = '-timestamp'
            modelkey = 'custom'
            

:parameter abstract: Check the :attr:`abstract` attribute.
:parameter ordering: Check the :attr:`ordering` attribute.
:parameter app_label: Check the :attr:`app_label` attribute.
:parameter keyprefix: Check the :attr:`keyprefix` attribute.
:parameter modelkey: Check the :attr:`modelkey` attribute.

**Attributes and methods**:

This is the list of attributes and methods available. All attributes,
but the ones mantioned above, are initialized by the object relational
mapper.

.. attribute:: abstract

    If ``True``, it represents an abstract model and no database elements
    are created.

.. attribute:: app_label

    Unless specified it is the name of the directory or file
    (if at top level) containing the
    :class:`stdnet.orm.StdModel` definition.
    
.. attribute:: model

    a subclass of :class:`stdnet.orm.StdModel`. Set by the ``orm``.
    
.. attribute:: ordering

    Optional name of a :class:`stdnet.orm.Field` in the :attr:`model`.
    If provided, indeces will be sorted with respect the value of the
    field specidied.
    Check the :ref:`sorting <sorting>` documentation for more details.
    
    Default: ``None``.
    
.. attribute:: dfields

    dictionary of :class:`stdnet.orm.Field` instances.
    
.. attribute:: fields

    list of :class:`stdnet.orm.Field` instances.
    
.. attribute:: keyprefix

    Override the :ref:`settings.DEFAULT_KEYPREFIX <settings>` value.
    
    Default ``None``.
    
.. attribute:: modelkey

    Override the modelkey which is by default given by ``app_label.name``
    
    Default ``None``.
        
.. attribute:: pk

    The primary key :class:`stdnet.orm.Field`
'''
    searchengine = None
    connection_string = None
    
    def __init__(self, model, fields,
                 abstract = False, keyprefix = None,
                 app_label = '', verbose_name = None,
                 ordering = None, modelkey = None,
                 **kwargs):
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
        self.modelkey = modelkey or '{0}.{1}'.format(self.app_label,self.name)
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
        return model.__new__(model)
        
    def __repr__(self):
        if self.app_label:
            return '%s.%s' % (self.app_label,self.name)
        else:
            return self.name
    
    def __str__(self):
        return self.__repr__()
        
    def basekey(self, *args):
        """Calculate the key to access model data in the backend server.
For example::
        
    >>> from examples.models import User
    >>> from orm import register
    >>> register(User)
    'redis db 7 on 127.0.0.1:6379'
    >>> User._meta.basekey()
    'stdnet.examples.user'
"""
        key = '{0}{1}'.format(self.keyprefix,self.modelkey)
        postfix = ':'.join((str(p) for p in args if p is not None))
        return '{0}:{1}'.format(key,postfix) if postfix else key
    
    def tempkey(self, name = None):
        return self.basekey('tmp',name or gen_unique_id())
    
    def autoid(self):
        '''The id for auto-increments ids'''
        return self.basekey('ids')
    
    def is_valid(self, instance):
        '''Perform validation for *instance* and stores serialized data,
indexes and errors into local cache.
Return ``True`` if the instance is ready to be saved to database.'''
        v = {}
        setattr(instance,'_validation',v)
        data = v['data'] = {}
        errors = v['errors'] = {}
        toload = v['toload'] = []
        indices = v['indices'] = []
        id = instance.id
        dbdata = instance._dbdata
        idnew = not (id and id == dbdata.get('id'))
        
        #Loop over scalar fields first
        for field,value in instance.fieldvalue_pairs():
            name = field.attname
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
                        #data[name] = svalue
                        data.update(svalue)
                    else:
                        if svalue is not None:
                            data[name] = svalue
                        # if the field is an index add it
                        if field.index:
                            if idnew:
                                indices.append((field,svalue,None))
                            else:
                                if field.name in dbdata:
                                    oldvalue = dbdata[field.name]
                                    if svalue != oldvalue:
                                        indices.append((field,svalue,oldvalue))
                                else:
                                    # The field was not loaded
                                    toload.append(field.name)
                                    indices.append((field,svalue,None))
                                
        return len(errors) == 0
                
    def table(self, transaction = None):
        '''Return an instance of :class:`stdnet.HashTable` holding
the model table'''
        if not self.cursor:
            raise ModelNotRegistered('%s not registered.\
 Call orm.register(model_class) to solve the problem.' % self)
        return self.cursor.hash(self.basekey(),self.timeout,
                                transaction=transaction)
    
    def flush(self):
        '''Fast method for clearing the whole table including related tables'''
        N = 0
        for rel in self.related.values():
            rmeta = rel._meta
            # This avoid circular reference
            if rmeta is not self:
                N += rmeta.flush()
        if self.cursor:
            N += self.cursor.flush(self)
        return N

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
            return orderinginfo(f.name,f,desc)
        else:
            if sortby in self.dfields:
                f = self.dfields[sortby]
                return orderinginfo(f.name,f,desc)
            sortbys = sortby.split(JSPLITTER)
            s0 = sortbys[0]
            if len(sortbys) > 1 and s0 in self.dfields:
                f = self.dfields[s0]
                return orderinginfo(sortby,f,desc)
        raise errorClass('Cannot Order by attribute "{0}".\
 It is not a scalar field.'.format(sortby))
        
    def server_fields(self, fields):
        '''Return a tuple containing a list
of fields names and a list of field attribute names.'''
        dfields = self.dfields
        processed = set()
        names = []
        atts = []
        for name in fields:
            if name == 'id':
                continue
            if name in processed:
                continue
            if name in dfields:
                processed.add(name)
                field = dfields[name]
                names.append(field.name)
                atts.append(field.attname)
            else:
                bname = name.split(JSPLITTER)[0]
                if bname in dfields:
                    field = dfields[bname]
                    if field.type == 'json object':
                        processed.add(name)
                        ames.append(name)
                        atts.append(name)
        return names,atts

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
                 modelkey = None,
                 **kwargs):
    return {'abstract': abstract,
            'keyprefix': keyprefix,
            'app_label':app_label,
            'ordering':ordering,
            'modelkey':modelkey}
    

