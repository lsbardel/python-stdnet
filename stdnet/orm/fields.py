import json
from copy import copy
from hashlib import sha1
import time
from datetime import date, datetime

from stdnet.exceptions import *
from stdnet.utils import pickle, json, json_compact, DefaultJSONEncoder,\
                         DefaultJSONHook, timestamp2date, date2timestamp,\
                         UnicodeMixin, novalue, to_string, is_string,\
                         to_bytestring, is_bytes_or_string, iteritems,\
                         encoders

from .related import RelatedObject, ReverseSingleRelatedObjectDescriptor
from .query import RelatedManager
from .globals import get_model_from_hash, JSPLITTER


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
           'ByteField',
           'ForeignKey',
           'JSONField',
           'PickleObjectField',
           'ModelField',
           'JSPLITTER']

EMPTY = ''


class Field(UnicodeMixin):
    '''This is the base class of all StdNet Fields.
Each field is specified as a :class:`stdnet.orm.StdModel` class attribute.
    
.. attribute:: index

    If ``True``, the field will create indexes for fast search.
    An index is implemented as a :class:`stdnet.Set`
    in the :class:`stdnet.BackendDataServer`. If you don't need to search
    the field you should set this value to ``False``.
    
    .. note:: if ``index`` is set to ``False`` executing queries
              againsT the field will
              throw a :class:`stdnet.QuerySetError` exception.
              No database queries are allowed for non indexed fields
              as a design decision (excplicit better than implicit).
    
    Default ``True``.
    
.. attribute:: unique

    If ``True``, the field must be unique throughout the model.
    In this case :attr:`Field.index` is also ``True``.
    Enforced at :class:`stdnet.BackendDataServer` level.
    
    Default ``False``.

.. attribute:: ordered

    If ``True``, the field will creates an ordering structure in the
    backend server.
    
    Default ``False``.
    
.. attribute:: primary_key

    If ``True``, this field is the primary key for the model.
    In this case :attr:`Field.unique` is also ``True``.
    
    Default ``False``.
    
.. attribute:: required

    If ``False``, the field is allowed to be null.
    
    Default ``True``.
    
.. attribute:: default

    Default value for this field. It can be a callable attribute.
    
    Default ``None``.
    
.. attribute:: name

    Field name, created by the ``orm`` at runtime.
    
.. attribute:: model

    The :class:`stdnet.orm.StdModel` holding the field.
    Created by the ``orm`` at runtime.
    
.. attribute:: charset

    The charset used for encoding decoding text.
    
.. attribute:: hidden

    If ``True`` the field will be hidden from search algorithms.
    
    Default ``False``.
'''
    default = None
    type = None
    index = True
    ordered = False
    charset = None
    hidden = False
    internal_type = None
    
    def __init__(self, unique = False, ordered = None, primary_key = False,
                 required = True, index = None, hidden = None,
                 **extras):
        self.primary_key = primary_key
        index = index if index is not None else self.index
        if primary_key:
            self.unique   = True
            self.required = True
            self.index    = True
        else:
            self.unique = unique
            self.required = required
            self.index = True if unique else index
        self.charset = extras.pop('charset',self.charset)
        self.ordered = ordered if ordered is not None else self.ordered
        self.hidden = hidden if hidden is not None else self.hidden
        self.meta = None
        self.name = None
        self.model = None
        self.as_cache = False
        self.default = extras.pop('default',self.default)
        self.encoder = self.get_encoder(extras)
        self._handle_extras(**extras)
        
    def _handle_extras(self, **extras):
        self.error_extras(extras)
        
    def get_encoder(self, params):
        return None
    
    def error_extras(self, extras):
        keys = list(extras)
        if keys:
            raise TypeError("__init__() got an unexepcted keyword\
 argument '{0}'".format(keys[0]))
        
    def __unicode__(self):
        return to_string('%s.%s' % (self.meta,self.name))
        
    def to_python(self, value):
        """Converts the input value into the expected Python
data type, raising :class:`stdnet.FieldValueError` if the data
can't be converted.
Returns the converted value. Subclasses should override this."""
        return value
    
    def value_from_data(self, data):
        return None
    
    def register_with_model(self, name, model):
        '''Called during the creation of a the :class:`stdnet.orm.StdModel`
class when :class:`stdnet.orm.base.Metaclass` is initialised. It fills
:attr:`Field.name` and :attr:`Field.model`. This is an internal
function users should never call.'''
        if self.name:
            raise FieldError('Field %s is already registered\
 with a model' % self)
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
    
    def serialize(self, value, transaction = None):
        '''Called by the :func:`stdnet.orm.StdModel.save` method when saving
an object to the remote data server. It returns a representation of *value*
to store in the database.
If an error occurs it raises :class:`stdnet.exceptions.FieldValueError`'''
        return self.scorefun(value)
    
    def add(self, *args, **kwargs):
        raise NotImplementedError("Cannot add to field")
    
    def id(self, obj):
        '''Field id for object *obj*, if applicable. Default is ``None``.'''
        return None
    
    def get_default(self):
        "Returns the default value for this field."
        if hasattr(self.default,'__call__'):
            return self.default()
        else:
            return self.default
    
    def index_value(self):
        '''A value which is used by indexes to generate keys.'''
        return self.value
    
    def scorefun(self, value):
        '''Function which evaluate a score from the field value. Used by
the ordering alorithm'''
        return value
    
    def scoreobject(self, obj):
        value = getattr(obj,self.name,None)
        return self.scorefun(value)
    
    def __deepcopy__(self, memodict):
        '''Nothing to deepcopy here'''
        field = copy(self)
        field.name = None
        field.model = None
        field.meta = None
        return field
            

class AtomField(Field):
    '''The base class for fields containing ``atoms``.
An atom is an irreducible
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
A symbol holds a unicode string as a single unit.
A symbol is irreducible, and are often used to hold names, codes
or other entities. They are indexes by default.'''
    type = 'text'
    internal_type = 'text'
    charset = 'utf-8'
    default = ''
    
    def get_encoder(self, params):
        return encoders.Default(self.charset)
    
    def to_python(self, value):
        if value is not None:
            return self.encoder.loads(value)
        else:
            return self.default
        
    def serialize(self, value, transaction = None):
        if value is not None:
            return self.encoder.dumps(value)
    
    def index_value(self):
        return sha1(self.value)
    

class IntegerField(AtomField):
    '''An integer :class:`AtomField`.'''
    type = 'integer'
    internal_type = 'numeric'
    #default = 0
    
    def scorefun(self, value):
        if value is not None:
            try:
                return int(value)
            except:
                raise FieldValueError('Field is not a valid integer')
        return value
    
    def to_python(self, value):
        if value is not None and value is not EMPTY:
            return int(value)
        else:
            return self.default
        
    
class BooleanField(AtomField):
    '''A boolean :class:`AtomField`'''
    type = 'bool'
    internal_type = 'numeric'
    
    def __init__(self, required = False, **kwargs):
        super(BooleanField,self).__init__(required = required,**kwargs)
    
    def scorefun(self, value):
        if value is None:
            return 0
        else:
            return 1 if int(value) else 0
        
    def to_python(self, value):
        return True if self.scorefun(value) else False
    
    def index_value(self):
        return 1 if self.value else 0
    
    
class AutoField(IntegerField):
    '''An :class:`IntegerField` that automatically increments.
You usually won't need to use this directly;
a ``primary_key`` field  of this type, named ``id``,
will automatically be added to your model
if you don't specify otherwise.
    '''
    type = 'auto'
    def serialize(self, value, transaction = None):
        if not value:
            value = self.meta.cursor.incr(self.meta.autoid())
        return super(AutoField,self).serialize(value)


class FloatField(AtomField):
    '''An floating point :class:`AtomField`. By default 
its :attr:`Field.index` is set to ``False``.
    '''
    type = 'float'
    internal_type = 'numeric'
    index = False
        
    def scorefun(self, value):
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
    '''An :class:`AtomField` represented in Python by
a :class:`datetime.date` instance.'''
    type = 'date'
    internal_type = 'numeric'
    ordered = True
    default = None
    
    def scorefun(self, value):
        if value is not None:
            if isinstance(value,date):
                value = date2timestamp(value)
            else:
                raise FieldValueError('Field %s is not a valid date' % self)
        return value
    
    def to_python(self, value):
        if value:
            if isinstance(value,date):
                if isinstance(value,datetime):
                    value = value.date()
            else:
                value = timestamp2date(float(value)).date()
        else:
            value = None
        return value
        
        
class DateTimeField(DateField):
    '''An date :class:`AtomField` represented in Python by
a :class:`datetime.datetime` instance.'''
    type = 'datetime'
    index = False
    
    def to_python(self, value):
        if value:
            if isinstance(value,date):
                if not isinstance(value,datetime):
                    value = datetime(value.year,value.month,value.day) 
            else:
                value = timestamp2date(float(value))
        else:
            value = None
        return value


class CharField(SymbolField):
    '''A text :class:`SymbolField` which is never an index.
It contains unicode and by default and :attr:`Field.required`
is set to ``False``.

It accept an additional attribute

.. attribute:: as_cache

    If ``True`` the field contains cached data.
    
    Default ``False``.
'''
    def __init__(self, *args, **kwargs):
        kwargs['index'] = False
        kwargs['unique'] = False
        kwargs['primary_key'] = False
        as_cache = kwargs.pop('as_cache',False)
        self.max_length = kwargs.pop('max_length',None) # not used for now 
        required = kwargs.get('required',None)
        if required is None:
            kwargs['required'] = False
        super(CharField,self).__init__(*args, **kwargs)
        self.as_cache = as_cache
    
    
class PickleObjectField(CharField):
    '''A field which implements automatic conversion to and form a pickable
python object.
This field is python specific and therefore not of much use
if accessed from external programs. Consider the :class:`ForeignKey`
or :class:`JSONField` fields as more general alternatives.'''
    type = 'object'
    internal_type = 'bytes'
    
    def get_encoder(self, params):
        return encoders.PythonPickle()
    
    def to_python(self, value):
        return self.encoder.loads(value)
    
    def scorefun(self, value):
        return self.encoder.dumps(value)
    

class ForeignKey(Field, RelatedObject):
    '''A field defining a one-to-many objects relationship.
Requires a positional argument: the class to which the model is related.
To create a recursive relationship, an object that has a many-to-one
relationship with itself use::

    orm.ForeignKey('self')

It accepts **related_name** as extra argument. It is the name to use for
the relation from the related object back to self. For example::

    class Folder(orm.StdModel):
        name = orm.SymobolField()
    
    class File(orm.StdModel):
        folder = orm.ForeignKey(Folder, related_name = 'files')
        
'''
    type = 'related object'
    internal_type = 'numeric'
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
        if not model._meta.abstract:
            setattr(model,self.name,ReverseSingleRelatedObjectDescriptor(self))
            self.register_with_related_model()
    
    def scorefun(self, value):
        raise NotImplementedError
    
    def serialize(self, value, transaction = None):
        try:
            return value.id
        except:
            return value
    
    
class JSONField(CharField):
    '''A JSON field which implements automatic converion to
and form a dictionary of data.
There are few extra parameters which can be used to customize the
behaviour and the storage of the JSON data.

:parameter encoder_class: The JSON class used for encoding.
                          A sensible default is available.
:parameter decoder_hook: A JSON decoder function.
                          A sensible default is available.
:parameter sep: A string separator for building nested JSON data.
                
                Default ``None``.
:parameter as_string: a boolean indicating if data should be serialized
                      into a string.
                      If the value is set to ``False``,
                      the JSON data is stored as a field
                      of the instance prefixed with the field name
                      and double underscore.
                      If ``True`` it is stored as a json string.
                    
                    Default ``True``.

For example, lets consider the following::

    class MyModel(orm.StdModel):
        name = orm.SymbolField()
        data = orm.JSONField(as_string = False)
    
And::

    >>> m = MyModel(name='bla',data={'mean':1,'std':3.5})
    >>> m.cleaned_data
    {'name':'bla','data__mean':'1','data__std':'3.5'}
    >>>
    
The only reason for setting ``as_string`` to ``False`` in a JSONfield
is that it enables sorting of instances with respect to its fields::

    >>> MyModel.objects.all().sort_by('data__std')

which can be rather useful.
'''
    type = 'json object'
    internal_type = 'serialized'
    def __init__(self, *args, **kwargs):
        kwargs['default'] = kwargs.get('default',{})
        self.encoder_class = kwargs.pop('encoder_class',DefaultJSONEncoder)
        self.decoder_hook  = kwargs.pop('decoder_hook',DefaultJSONHook)
        self.sep = kwargs.pop('sep',None)
        self.as_string = kwargs.pop('as_string',True)
        super(JSONField,self).__init__(*args, **kwargs)
        
    def to_python(self, value):
        if value is not None and not isinstance(value,dict):
            value = to_string(value)
            if not value:
                value = {}
            else:
                value = self.loads(value)
        return value
    
    def serialize(self, value, transaction = None):
        if value is not None:
            if is_bytes_or_string(value):
                value = self.to_python(value)
            if self.as_string:
                value = self.dumps(json_compact(value,self.sep))
            else:
                name = self.name
                dumps = self.dumps
                value = dict((('{0}{1}{2}'.\
                               format(name,JSPLITTER,field),dumps(v))\
                              for field,v in iteritems(value)))
        return value
    
    def value_from_data(self, data):
        if not self.as_string:
            name = self.name
            loads = self.loads
            val = {}
            for k,v in iteritems(data):
                ks = k.split(JSPLITTER)
                if len(ks) > 1 and ks[0] == name:
                    val[JSPLITTER.join(ks[1:])] = loads(to_string(v))
            return val
    
    def dumps(self, value):
        return json.dumps(value, cls=self.encoder_class)
    
    def loads(self, svalue):
        return json.loads(svalue, object_hook = self.decoder_hook)


class ByteField(CharField):
    '''A field which contains binary data.
In python this is converted to `bytes`.'''
    internal_type = 'bytes'
    
    def get_encoder(self, params):
        return encoders.Bytes(self.charset)
        

class ModelField(SymbolField):
    '''A filed which can be used to store the model classes (not only
:class:`stdnet.orm.StdModel` models). If a class has a attribute ``_meta``
with a unique hash attribute ``hash`` and it is
registered in the model hash table, it can be used.'''
    type = 'model'
    internal_type = 'text'
    
    def to_python(self, value):
        if value:
            value = self.encoder.loads(value)
            return get_model_from_hash(value)
    
    def serialize(self, value):
        if value is not None:
            if not hasattr(value,'_meta'):
                value = self.to_python(value)
                if not hasattr(value,'_meta'):
                    return
            value = value._meta.hash
            return self.encoder.dumps(value)

