from copy import copy
from hashlib import sha1
import time
from datetime import date, datetime

from query import RelatedManager
from related import RelatedObject
from stdnet.exceptions import *
from stdnet.utils import timestamp2date, date2timestamp


hashfun = lambda x : sha1(x).hexdigest()


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
           '_novalue']

class NoValue(object):
    pass

_novalue = NoValue()


class Field(object):
    '''This is the base class of of StdNet Fields.
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
    
    Default :class:`NoValue`.
    
.. attribute:: name

    Field name, created by the ``orm`` at runtime.
    
.. attribute:: model

    The :class:`stdnet.orm.StdModel` holding the field.
    Created by the ``orm`` at runtime. 
'''
    default=NoValue
    
    def __init__(self, unique = False, ordered = False, primary_key = False,
                 required = True, index = True, default=NoValue):
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
        self.default  = default if default is not NoValue else self.default
        
    def __str__(self):
        return '%s.%s' % (self.meta,self.name)
    
    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__,self)
        
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
        self.model = model
        self.meta  = model._meta
        self.meta.dfields[name] = self
        if name is not 'id':
            self.meta.fields.append(self)
    
    def get_full_value(self):
        '''Return the expanded value of the field. For standard fields this is the
same as the field value, while for more complex fields, such as :class:`ForeignKey`, it
get extra data from the database. This function is called by the model when accessing
fields values.'''
        return self._value
    
    def serialize(self, value):
        '''Called by the :func:'stdnet.orm.StdModel.save` method when saving
an object to the remote data server. It return s a serializable representation of *value*.
If an error occurs it raises :class:`stdnet.exceptions.FieldValueError`'''
        return value
    
    def isvalid(self):
        '''Return ``True`` if Field is valid otherwise raise a ``FieldError`` exception.'''
        name    = self.name
        obj     = self.obj
        value   = self.serialize()
        if value is None and self.required:
            raise FieldError('Field %s for %s has no value' % (name,obj))
        if self.primary_key:
            setattr(obj,name,value)
        return True
    
    def add(self, *args, **kwargs):
        raise NotImplementedError("Cannot add to field")
    
    def id(self, obj):
        '''Field id for object *obj*, if applicable. Default is ``None``.'''
        return None
    
    def has_default(self):
        "Returns a boolean of whether this field has a default value."
        return self.default is not NoValue
    
    def get_default(self):
        "Returns the default value for this field."
        if self.has_default():
            if callable(self.default):
                return self.default()
            else:
                return self.default
        return None
    
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
    type = 'text'


class SymbolField(AtomField):
    '''A :class:`AtomField` which contains a ``symbol``.
A symbol holds a sequence of characters as a single unit.
A symbol is irreducible, and are often used to hold names
of other entities.'''
    def serialize(self, value):
        if value is not None:
            value = str(value)
        return value


class IntegerField(AtomField):
    '''An integer :class:`AtomField`.'''
    type = 'integer'
    def serialise(self, value):
        if value is not None:
            try:
                return int(value)
            except:
                raise FieldValueError('Field is not a valid integer')
        return value
    
    
class BooleanField(AtomField):
    '''An boolean :class:`AtomField`'''
    type = 'bool'
    def serialise(self, value):
        return True if value else False
        
    
class AutoField(IntegerField):
    '''An :class:`IntegerField` that automatically increments.
You usually won't need to use this directly;
a primary key field will automatically be added to your model
if you don't specify otherwise.
    '''            
    def serialize(self, value):
        if not value:
            value = self.meta.cursor.incr(self.meta.autoid())
        return super(AutoField,self).serialise(value)


class FloatField(AtomField):
    '''An floating point :class:`AtomField`. By default 
its :attr:`Field.index` is set to ``False``.
    '''
    type = 'float'
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
    
    
class DateField(AtomField):
    '''An date :class:`AtomField` represented in Python by
a :class:`datetime.date` instance.'''
    type = 'date'
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
        
        
class DateTimeField(AtomField):
    '''An date :class:`AtomField` represented in Python by
a :class:`datetime.datetime` instance.'''
    type = 'datetime'
    def serialize(self, value):
        if value is not None:
            if isinstance(value,date):
                value = timestamp2date(value)
            else:
                raise FieldValueError('Field %s is not a valid datetime' % self)
        return value
    
    def to_python(self, value):
        if value:
            value = timestamp2date(value)
        return value


class CharField(Field):
    '''A text :class:`Field` which is never an index.
It contains strings and by default :attr:`Field.required`
is set to ``False``.'''
    default = ''
    def __init__(self, *args, **kwargs):
        kwargs['index'] = False
        kwargs['unique'] = False
        kwargs['primary_key'] = False
        kwargs['ordered'] = False
        required = kwargs.get('required',None)
        if required is None:
            kwargs['required'] = False
        super(CharField,self).__init__(*args, **kwargs)
        
    def serialize(self, value):
        if value is not None:
            value = str(value)
        return value
    

class ForeignKey(Field, RelatedObject):
    '''A field defining a one-to-many objects relationship.
The StdNet equivalent to `django ForeignKey <http://docs.djangoproject.com/en/dev/ref/models/fields/#foreignkey>`_.
Requires a positional argument: the class to which the model is related.
To create a recursive relationship, an object that has a many-to-one relationship with itself,
use::

    orm.ForeignKey('self')

It accepts **related_name** as extra argument. It is the name to use for the relation from the related object
back to self. For example::

    class Folder(orm.StdModel):
        name = orm.AtomField()
    
    class File(orm.StdModel):
        folder = orm.ForeignKey(Folder, related_name = 'files')
        
'''        
    def __init__(self, model, related_name = None, **kwargs):
        Field.__init__(self, **kwargs)
        RelatedObject.__init__(self,
                               model,
                               relmanager = RelatedManager,
                               related_name = related_name)
        self.__value_obj = _novalue
        self.index = True
    
    def set_value(self, name, obj, value):
        value = super(ForeignKey,self).set_value(name,obj,value)
        if isinstance(value,self.model):
            self.__value_obj = value
            self._value = value.id
    
    def get_full_value(self):
        v = self.__value_obj
        if isinstance(v,NoValue):
            if self._value:
                meta    = self.model._meta
                hash    = meta.cursor.hash(meta.basekey())
                v       = hash.get(self._value)
            else:
                v = None
            self.__value_obj = v
        return v
    
    def get_value(self, value):
        if isinstance(value,self.model):
            return value.id
        else:
            return value
    
    def hash(self, value):
        return self.get_value(value)
    