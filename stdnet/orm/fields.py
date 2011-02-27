from copy import copy
from hashlib import sha1
import time
from datetime import date, datetime

from stdnet.exceptions import *
from stdnet.utils import pickle, json, json_compact, DefaultJSONEncoder,\
                         DefaultJSONHook, timestamp2date, date2timestamp,\
                         UnicodeMixin, novalue, to_string, is_string

from .related import RelatedObject, ReverseSingleRelatedObjectDescriptor
from .query import RelatedManager
from .globals import get_model_from_hash


__all__ = ['Field',
           'AutoField',
           'AtomField',
           'IntegerField',
           'BooleanField',
           'FloatField',
           'DateField',
           'DateTimeField',
           'SymbolField',
           'CharField',
           'ForeignKey',
           'JSONField',
           'PickleObjectField',
           'ModelField']


class Field(UnicodeMixin):
    '''This is the base class of all StdNet Fields.
Each field is specified as a :class:`stdnet.orm.StdModel` class attribute.
    
.. attribute:: index

    If ``True``, the field will create indexes for fast search.
    An index is implemented as a :class:`stdnet.Set`
    in the :class:`stdnet.BackendDataServer`. If you don't need to search
    the field you should set this value to ``False``.
    
    Default ``True``.
    
.. attribute:: unique

    If ``True``, the field must be unique throughout the model.
    In this case :attr:`Field.index` is also ``True``.
    Enforced at :class:`stdnet.BackendDataServer` level.
    
    Default ``False``.

.. attribute:: ordered

    If ``True``, the field is ordered. if :attr:`Field.unique` is ``True`` this has no effect.
    
    Default ``False``.
    
.. attribute:: primary_key

    If ``True``, this field is the primary key for the model.
    In this case :attr:`Field.unique` is also ``True``.
    
    Default ``False``.
    
.. attribute:: required

    If ``False``, the field is allowed to be null.
    
    Default ``True``.
    
.. attribute:: default

    Default value for this field.
    
    Default ``novalue``.
    
.. attribute:: name

    Field name, created by the ``orm`` at runtime.
    
.. attribute:: model

    The :class:`stdnet.orm.StdModel` holding the field.
    Created by the ``orm`` at runtime. 
'''
    default=novalue
    
    def __init__(self, unique = False, ordered = False, primary_key = False,
                 required = True, index = True, default=novalue, **extras):
        self.primary_key = primary_key
        if primary_key:
            self.unique   = True
            self.required = True
            self.index    = True
        else:
            self.unique = unique
            self.required = required
            self.index = True if unique else index
        self.ordered  = ordered
        self.meta     = None
        self.name     = None
        self.model    = None
        self.default  = default if default is not novalue else self.default
        self._handle_extras(**extras)
        
    def _handle_extras(self, **extras):
        self.error_extras(extras)
        
    def error_extras(self, extras):
        keys = list(extras)
        if keys:
            raise TypeError("__init__() got an unexepcted keyword argument '{0}'".format(keys[0]))
        
    def __unicode__(self):
        return to_string('%s.%s' % (self.meta,self.name))
        
    def to_python(self, value):
        """Converts the input value into the expected Python
data type, raising :class:`stdnet.FieldValueError` if the data
can't be converted.
Returns the converted value. Subclasses should override this."""
        return value
    
    def register_with_model(self, name, model):
        '''Called during the creation of a the :class:`stdnet.orm.StdModel`
class when :class:`stdnet.orm.base.Metaclass` is initialised. It fills
:attr:`Field.name` and :attr:`Field.model`. This is an internal
function users should never call.'''
        if self.name:
            raise FieldError('Field %s is already registered with a model' % self)
        self.name  = name
        self.attname =self.get_attname()
        self.model = model
        meta = model._meta
        self.meta  = meta
        meta.dfields[name] = self
        meta.fields.append(self)
        if name is not 'id':
            self.add_to_fields()
            
    def add_to_fields(self):
        self.model._meta.scalarfields.append(self)
    
    def get_attname(self):
        return self.name
    
    def get_cache_name(self):
        return '_%s_cache' % self.name
    
    def serialize(self, value):
        '''Called by the :func:`stdnet.orm.StdModel.save` method when saving
an object to the remote data server. It return s a serializable representation of *value*.
If an error occurs it raises :class:`stdnet.exceptions.FieldValueError`'''
        return value
    
    def add(self, *args, **kwargs):
        raise NotImplementedError("Cannot add to field")
    
    def id(self, obj):
        '''Field id for object *obj*, if applicable. Default is ``None``.'''
        return None
    
    def has_default(self):
        "Returns a boolean of whether this field has a default value."
        return self.default is not novalue
    
    def get_default(self):
        "Returns the default value for this field."
        if self.has_default():
            if hasattr(self.default,'__call__'):
                return self.default()
            else:
                return self.default
        return None
    
    def index_value(self):
        '''A value which is used by indexes to generate keys.'''
        return self.value
    
    def __deepcopy__(self, memodict):
        '''Nothing to deepcopy here'''
        field = copy(self)
        field.name = None
        field.model = None
        field.meta = None
        return field
            

class AtomField(Field):
    '''The base class for fields containing ``atoms``. An atom is an irreducible
value with a specific data type. it can be of four different types:

* boolean
* integer
* date
* datetime
* floating point
* symbol
'''
    pass
        
    

class SymbolField(AtomField):
    '''An :class:`AtomField` which contains a ``symbol``.
A symbol holds a sequence of characters as a single unit.
A symbol is irreducible, and are often used to hold names, codes
or other entities. They are indexes by default.'''
    type = 'symbol'
    default = to_string('')
    
    def to_python(self, value):
        if value is not None:
            return to_string(value)
        else:
            return self.default
    
    def index_value(self):
        return sha1(self.value)
    

class IntegerField(AtomField):
    '''An integer :class:`AtomField`.'''
    type = 'integer'
    default = 0
    def serialise(self, value):
        if value is not None:
            try:
                return int(value)
            except:
                raise FieldValueError('Field is not a valid integer')
        return value
    
    def to_python(self, value):
        if value:
            return int(value)
        else:
            return self.default
        
    
    
class BooleanField(AtomField):
    '''An boolean :class:`AtomField`'''
    type = 'bool'
    def serialise(self, value):
        return True if value else False
        
    def to_python(self, value):
        return True if value else False
    
    def index_value(self):
        return 1 if self.value else 0
    
    
class AutoField(IntegerField):
    '''An :class:`IntegerField` that automatically increments.
You usually won't need to use this directly;
a primary key field will automatically be added to your model
if you don't specify otherwise.
    '''
    type = 'auto'            
    def serialize(self, value):
        if not value:
            value = self.meta.cursor.incr(self.meta.autoid())
        return super(AutoField,self).serialise(value)


class FloatField(AtomField):
    '''An floating point :class:`AtomField`. By default 
its :attr:`Field.index` is set to ``False``.
    '''
    type = 'float'
    default = 0.
    def __init__(self,*args,**kwargs):
        index = kwargs.get('index',None)
        if index is None:
            kwargs['index'] = False
        super(FloatField,self).__init__(*args,**kwargs)
        
    def serialize(self, value):
        if value is not None:
            try:
                return float(value)
            except:
                raise FieldValueError('Field is not a valid float')
        return value
    
    def to_python(self, value):
        if value:
            return float(value)
        else:
            return self.default
    
    
class DateField(AtomField):
    '''An date :class:`AtomField` represented in Python by
a :class:`datetime.date` instance.'''
    type = 'date'
    default = None
    def serialize(self, value):
        if value is not None:
            if isinstance(value,date):
                value = date2timestamp(value)
            else:
                raise FieldValueError('Field %s is not a valid date' % self)
        return value
    
    def to_python(self, value):
        if value:
            value = timestamp2date(value).date()
        return value
        
        
class DateTimeField(DateField):
    '''An date :class:`AtomField` represented in Python by
a :class:`datetime.datetime` instance.'''
    type = 'datetime'
    
    def to_python(self, value):
        if value:
            value = timestamp2date(value)
        return value


class CharField(SymbolField):
    '''A text :class:`Field` which is never an index.
It contains strings and by default :attr:`Field.required`
is set to ``False``.'''
    type = 'text'
    def __init__(self, *args, **kwargs):
        kwargs['index'] = False
        kwargs['unique'] = False
        kwargs['primary_key'] = False
        kwargs['ordered'] = False
        self.max_length = kwargs.pop('max_length',None) # not used for now 
        required = kwargs.get('required',None)
        if required is None:
            kwargs['required'] = False
        super(CharField,self).__init__(*args, **kwargs)
        
    def serialize(self, value):
        if value is not None:
            value = str(value)
        return value
    
    
class PickleObjectField(CharField):
    type = 'object'
    def to_python(self, value):
        if value is None:
            return value
        elif isinstance(value, basestring):
            return pickle.loads(value)
        else:
            return value
    
    def serialize(self, value):
        if value is not None:
            value = pickle.dumps(value)
        return value
    

class ForeignKey(Field, RelatedObject):
    '''A field defining a one-to-many objects relationship.
Requires a positional argument: the class to which the model is related.
To create a recursive relationship, an object that has a many-to-one relationship with itself,
use::

    orm.ForeignKey('self')

It accepts **related_name** as extra argument. It is the name to use for the relation from the related object
back to self. For example::

    class Folder(orm.StdModel):
        name = orm.SymobolField()
    
    class File(orm.StdModel):
        folder = orm.ForeignKey(Folder, related_name = 'files')
        
'''        
    def __init__(self, model, related_name = None, **kwargs):
        Field.__init__(self, **kwargs)
        RelatedObject.__init__(self,
                               model,
                               relmanager = RelatedManager,
                               related_name = related_name)
    
    def get_attname(self):
        return '%s_id' % self.name
    
    def register_with_model(self, name, model):
        super(ForeignKey,self).register_with_model(name, model)
        setattr(model,self.name,ReverseSingleRelatedObjectDescriptor(self))
        self.register_with_related_model()
    
    def serialize(self, value):
        try:
            return value.id
        except:
            return value
    
    
class JSONField(CharField):
    '''A JSON field which implements authomatic converion to and form dictionary of data.'''
    type = 'json object'
    def __init__(self, *args, **kwargs):
        kwargs['default'] = kwargs.get('default',{})
        self.encoder_class = kwargs.pop('encoder_class',DefaultJSONEncoder)
        self.decoder_hook  = kwargs.pop('decoder_hook',DefaultJSONHook)
        self.sep = kwargs.pop('sep',None)
        super(JSONField,self).__init__(*args, **kwargs)
        
    def to_python(self, value):
        if is_string(value):
            if not value:
                value = {}
            else:
                value = json.loads(value, object_hook = self.decoder_hook)
        return value
    
    def serialize(self, value):
        if value is not None:
            value = json.dumps(json_compact(value,self.sep), cls=self.encoder_class)
        return value
    

class ModelField(SymbolField):
    '''A filed which can be used to store the model unique sha1'''
    
    def to_python(self, value):
        return get_model_from_hash(value)
    
    def serialize(self, value):
        if value is not None:
            value = value._meta.hash
        return value

