import sys
import copy
import hashlib
import weakref

from stdnet import BackendRequest
from stdnet.utils import zip, to_bytestring, to_string, UnicodeMixin
from stdnet.exceptions import *

from . import signals
from .globals import hashmodel, JSPLITTER, get_model_from_hash
from .fields import Field, AutoField, orderinginfo
from .session import Manager, setup_managers


__all__ = ['Metaclass','Model','ModelBase','StdNetType', 'from_uuid']


def get_fields(bases, attrs):
    fields = {}
    for base in bases:
        if hasattr(base, '_meta'):
            fields.update(copy.deepcopy(base._meta.dfields))
    
    for name,field in list(attrs.items()):
        if isinstance(field,Field):
            fields[name] = attrs.pop(name)
    
    return fields


class ModelMeta(object):
    '''A class for storing meta data of a :class:`Model` class.'''
    def __init__(self, model, app_label = None, modelkey = None,
                 abstract = False):
        self.abstract = abstract
        self.model = model
        self.model._meta = self
        self.app_label = app_label
        self.name = model.__name__.lower()
        if not modelkey:
            if self.app_label:
                modelkey = '{0}.{1}'.format(self.app_label,self.name)
            else:
                modelkey = self.name
        self.modelkey = modelkey
        hashmodel(model)
        
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
    
    def pk_to_python(self, id):
        return to_string(id)
    
        
class Metaclass(ModelMeta):
    '''An instance of :class:`Metaclass` stores all information
which maps an :class:`StdModel` into an object in the in a remote
:class:`stdnet.BackendDataServer`.
An instance is initiated by the orm when a :class:`StdModel` class is created.

To override default behaviour you can specify the ``Meta`` class as an inner
class of :class:`StdModel` in the following way::

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
    (if at top level) containing the :class:`StdModel` definition.
    
.. attribute:: model

    The :class:`StdModel` represented by the :class:`Metaclass`.
    This attribute is set by the ``orm`` during class initialization.
    
.. attribute:: ordering

    Optional name of a :class:`stdnet.orm.Field` in the :attr:`model`.
    If provided, indeces will be sorted with respect the value of the
    field specidied.
    Check the :ref:`sorting <sorting>` documentation for more details.
    
    Default: ``None``.
    
.. attribute:: dfields

    dictionary of :class:`stdnet.orm.Field` instances.
    
.. attribute:: fields

    list of all :class:`Field` instances.
    
.. attribute:: indices

    List of :class:`Field` which are indices (:attr:`Field.index` attribute
    set to ``True``).
    
.. attribute:: modelkey

    Override the modelkey which is by default given by ``app_label.name``
    
    Default ``None``.
        
.. attribute:: pk

    The :class:`Field` representing the primary key.
'''
    searchengine = None
    connection_string = None
    
    def __init__(self, model, fields,
                 abstract = False, app_label = '',
                 verbose_name = None,
                 ordering = None, modelkey = None,
                 **kwargs):
        super(Metaclass,self).__init__(model,
                                       app_label = app_label,
                                       modelkey = modelkey,
                                       abstract = abstract)
        self.fields = []
        self.scalarfields = []
        self.indices = []
        self.multifields = []
        self.dfields = {}
        self.timeout = 0
        self.related = {}
        self.verbose_name = verbose_name or self.name
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
    
    def pk_to_python(self, id):
        return self.pk.to_python(id)
    
    def is_valid(self, instance):
        '''Perform validation for *instance* and stores serialized data,
indexes and errors into local cache.
Return ``True`` if the instance is ready to be saved to database.'''
        dbdata = instance._dbdata
        data = dbdata['cleaned_data'] = {}
        errors = dbdata['errors'] = {}
        
        #Loop over scalar fields first
        for field,value in instance.fieldvalue_pairs():
            name = field.attname
            if not name:
                continue
            value = instance.set_field_value(field, value)
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
                                
        return len(errors) == 0
    
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

    def get_sorting(self, sortby, errorClass):
        s = None
        desc = False
        if sortby.startswith('-'):
            desc = True
            sortby = sortby[1:]
        if sortby == 'id':
            f = self.pk
            return orderinginfo(f.attname, f, desc, self.model, None)
        else:
            if sortby in self.dfields:
                f = self.dfields[sortby]
                return orderinginfo(f.attname, f, desc, self.model, None)
            sortbys = sortby.split(JSPLITTER)
            s0 = sortbys[0]
            if len(sortbys) > 1 and s0 in self.dfields:
                f = self.dfields[s0]
                nested = f.get_sorting(JSPLITTER.join(sortbys[1:]),errorClass)
                if nested:
                    sortby = f.attname
                return orderinginfo(sortby, f, desc, self.model, nested)
        raise errorClass('Cannot Order by attribute "{0}".\
 It is not a scalar field.'.format(sortby))
        
    def backend_fields(self, fields):
        '''Return a tuple containing a list
of fields names and a list of field attribute names.'''
        dfields = self.dfields
        processed = set()
        names = []
        atts = []
        for name in fields:
            if name == 'id' or name in processed:
                continue
            elif name in dfields:
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
                        names.append(name)
                        atts.append(name)
        return names,atts


    def multifields_ids_todelete(self, instance):
        '''Return the list of ids of :class:`MultiField` belonging to *instance*
which needs to be deleted when *instance* is deleted.'''
        gen = (field.id(instance) for field in self.multifields\
                                         if field.todelete())
        return [fid for fid in gen if fid]
        
    
class ModelType(type):
    '''StdModel python metaclass'''
    is_base_class = True
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, ModelType)]
        if not parents or attrs.pop('is_base_class',False):
            return super(ModelType, cls).__new__(cls, name, bases, attrs)
        return cls.make(name, bases, attrs, attrs.pop('Meta', None))
    
    @classmethod    
    def make(cls, name, bases, attrs, meta):
        model = type.__new__(cls, name, bases, attrs)
        meta = ModelMeta(model)
        return model


class StdNetType(ModelType):
    is_base_class = True
    @classmethod
    def make(cls, name, bases, attrs, meta):
        if meta:
            kwargs   = meta_options(**meta.__dict__)
        else:
            kwargs   = meta_options()
            
        # remove and build field list
        fields    = get_fields(bases, attrs)        
        # create the new class
        new_class = type.__new__(cls, name, bases, attrs)
        setup_managers(new_class)
        app_label = kwargs.pop('app_label')
        
        if app_label is None:
            model_module = sys.modules[new_class.__module__]
            try:
                app_label = model_module.__name__.split('.')[-2]
            except:
                app_label = ''
        
        meta = Metaclass(new_class,fields,app_label=app_label,**kwargs)
        signals.class_prepared.send(sender=new_class)
        return new_class
    

def meta_options(abstract = False,
                 app_label = None,
                 ordering = None,
                 modelkey = None,
                 **kwargs):
    return {'abstract': abstract,
            'app_label':app_label,
            'ordering':ordering,
            'modelkey':modelkey}
    

class ModelState(object):
    __slots__ = ('persistent','deleted','iid')
    def __init__(self, instance):
        self.persistent = False
        self.deleted = False
        dbdata = instance._dbdata
        if instance.id and 'id' in dbdata:
            if instance.id != dbdata['id']:
                raise ValueError('Id has changed from {0} to {1}.'\
                                 .format(instance.id,dbdata['id']))
            self.persistent = True
            self.iid = instance.id
        else:
            self.iid = instance.id or 'new.{0}'.format(id(instance)) 
    
    @property
    def action(self):
        return 'delete' if self.deleted else 'save'
    
    def __repr__(self):
        if self.persistent:
            return 'persistent' + (' deleted' if self.deleted else '')
        else:
            return 'new'
    __str__ = __repr__
    
    
class Model(UnicodeMixin):
    '''A mixin class for :class:`StdModel`. It implements the :attr:`uuid`
attribute which provides the univarsal unique identifier for an instance of a
model.'''
    _model_type = None
    DoesNotExist = ObjectNotFound
    '''Exception raised when an instance of a model does not exist.'''
    DoesNotValidate = ObjectNotValidated
    '''Exception raised when an instance of a model does not validate. Usually
raised when trying to save an invalid instance.'''
    
    def __new__(cls, id = None, **kwargs):
        o = super(Model,cls).__new__(cls)
        o.id = id
        o._dbdata = {}
        return o
        
    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return self.id == other.id
        else:
            return False
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        if self.id:
            return hash(self.uuid)
        else:
            return id(self)
    
    def state(self, update = False):
        if 'state' not in self._dbdata or update:
            self._dbdata['state'] = ModelState(self)
        return self._dbdata['state']
    
    @classmethod
    def get_uuid(cls, id):
        return '{0}.{1}'.format(cls._meta.hash,id)
        
    @property
    def uuid(self):
        '''Universally unique identifier for an instance.'''
        if not self.id:
            raise self.DoesNotExist(\
                    'Object not saved. Cannot obtain universally unique id')
        return self.get_uuid(self.id)
    
    def __get_session(self):
        return self._dbdata.get('session')
    def __set_session(self,session):
        self._dbdata['session'] = session
    session = property(__get_session,__set_session)
    
    def get_session(self):
        session = self.session
        if session is None:
            raise ValueError('No session available')
        else:
            return session
        
    def save(self):
        '''A fast method for saving an object. Use this method with care
since it commits changes to the backend database immediately. If a session
is not available, it tries to create one from its :class:`Manager`.'''
        session = self.get_session()
        with session.begin():
            session.add(self)
        return self
    
    def delete(self):
        session = self.get_session()
        with session.begin():
            session.delete(self)
        return self
    
    def async_handle(self, result, callback, *args, **kwargs):
        if isinstance(result,BackendRequest):
            return result.add_callback(lambda res :\
                        self.async_callback(callback, res, *args, **kwargs))
        else:
            return self.async_callback(callback, result, *args, **kwargs)
        
    def async_callback(self, callback, result, *args, **kwargs):
        if isinstance(result, Exception):
            raise result
        else:
            return callback(result, *args, **kwargs)
    
    
ModelBase = ModelType('ModelBase',(Model,),{'is_base_class': True})


def from_uuid(uuid, session = None):
    '''Retrieve a :class:`Model` from its universally unique identifier
*uuid*. If the *uuid* does not match any instance an exception will raise.'''
    elems = uuid.split('.')
    if len(elems) == 2:
        model = get_model_from_hash(elems[0])
        if not model:
            raise Model.DoesNotExist(\
                        'model id "{0}" not available'.format(elems[0]))
        if not session:
            session = model.objects.session()
        return session.query(model).get(id = elems[1])
    raise Model.DoesNotExist('uuid "{0}" not recognized'.format(uuid))
