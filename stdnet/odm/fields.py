import logging
from copy import copy
from hashlib import sha1
from collections import namedtuple
import time
from datetime import date, datetime
from base64 import b64encode

from stdnet import range_lookups
from stdnet.exceptions import *
from stdnet.utils import pickle, DefaultJSONEncoder,\
                         DefaultJSONHook, timestamp2date, date2timestamp,\
                         UnicodeMixin, to_string, is_string,\
                         is_bytes_or_string, iteritems,\
                         encoders, flat_to_nested, dict_flat_generator,\
                         string_type

from . import related
from .globals import get_model_from_hash, get_hash_from_model, JSPLITTER


orderinginfo = namedtuple('orderinginfo','name field desc model nested, auto')

logger = logging.getLogger('stdnet.odm')

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
           'ManyToManyField',
           'CompositeIdField',
           'JSPLITTER']

NONE_EMPTY = (None,'')


def field_value_error(f):

    def _(self, value):
        try:
            return f(self, value)
        except FieldValueError:
            raise
        except:
            raise FieldValueError('%s not valid for "%s"' % (value, self.name))

    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _


class Field(UnicodeMixin):
    '''This is the base class of all StdNet Fields.
Each field is specified as a :class:`StdModel` class attribute.

.. attribute:: index

    Probably the most important field attribute, it establish if
    the field creates indexes for queries.
    If you don't need to query the field you should set this value to
    ``False``, it will save you memory.

    .. note:: if ``index`` is set to ``False`` executing queries
              against the field will
              throw a :class:`stdnet.QuerySetError` exception.
              No database queries are allowed for non indexed fields
              as a design decision (explicit better than implicit).

    Default ``True``.

.. attribute:: unique

    If ``True``, the field must be unique throughout the model.
    In this case :attr:`Field.index` is also ``True``.
    Enforced at :class:`stdnet.BackendDataServer` level.

    Default ``False``.

.. attribute:: primary_key

    If ``True``, this field is the primary key for the model.
    A primary key field has the following properties:

    * :attr:`Field.unique` is also ``True``.
    * There can be only one in a model.
    * It's attribute name in the model must be **id**.
    * If not specified a :class:`AutoField` will be added.

    Default ``False``.

.. attribute:: required

    If ``False``, the field is allowed to be null.

    Default ``True``.

.. attribute:: default

    Default value for this field. It can be a callable attribute with arity 0.

    Default ``None``.

.. attribute:: name

    Field name, created by the ``odm`` at runtime.

.. attribute:: attname

    The attribute name for the field, created by the :meth:`get_attname` method
    at runtime. For most field, its value is the same as the :attr:`name`.
    It is the field sorted in the backend database.

.. attribute:: model

    The :class:`StdModel` holding the field.
    Created by the ``odm`` at runtime.

.. attribute:: charset

    The charset used for encoding decoding text.

.. attribute:: hidden

    If ``True`` the field will be hidden from search algorithms.

    Default ``False``.

.. attribute:: python_type

    The python ``type`` for the :class:`Field`.

.. attribute:: as_cache

    If ``True`` the field contains data which is considered cache and
    therefore always reproducible. Field marked as cache, have :attr:`required`
    always ``False``.

    This attribute is used by the :class:`StdModel.fieldvalue_pairs` method
    which returns a dictionary of field names and values.

    Default ``False``.
'''
    default = None
    type = None
    python_type = None
    index = True
    ordered = False
    charset = None
    hidden = False
    internal_type = None

    def __init__(self, unique=False, ordered=None, primary_key=False,
                 required=True, index=None, hidden=None, as_cache=False,
                 **extras):
        self.primary_key = primary_key
        index = index if index is not None else self.index
        if primary_key:
            self.unique = True
            self.required = True
            self.index = True
            self.as_cache = False
        else:
            self.unique = unique
            self.required = required
            self.as_cache = as_cache
            self.index = True if unique else index
        if self.as_cache:
            self.required = False
            self.unique = False
            self.index = False
        self.charset = extras.pop('charset',self.charset)
        self.ordered = ordered if ordered is not None else self.ordered
        self.hidden = hidden if hidden is not None else self.hidden
        self.meta = None
        self.name = None
        self.model = None
        self.default = extras.pop('default',self.default)
        self.encoder = self.get_encoder(extras)
        self._handle_extras(**extras)

    def get_encoder(self, params):
        return None

    def error_extras(self, extras):
        keys = list(extras)
        if keys:
            raise TypeError("__init__() got an unexepcted keyword\
 argument '{0}'".format(keys[0]))

    def __unicode__(self):
        return to_string('%s.%s' % (self.meta,self.name))

    def value_from_data(self, instance, data):
        return data.pop(self.attname,None)

    def register_with_model(self, name, model):
        '''Called during the creation of a the :class:`StdModel`
class when :class:`Metaclass` is initialised. It fills
:attr:`Field.name` and :attr:`Field.model`. This is an internal
function users should never call.'''
        if self.name:
            raise FieldError('Field %s is already registered\
 with a model' % self)
        self.name  = name
        self.attname = self.get_attname()
        self.model = model
        meta = model._meta
        self.meta  = meta
        meta.dfields[name] = self
        meta.fields.append(self)
        if not self.primary_key:
            self.add_to_fields()

    def add_to_fields(self):
        meta = self.model._meta
        meta.scalarfields.append(self)
        if self.index:
            meta.indices.append(self)

    def get_attname(self):
        '''Generate the :attr:`attname` at runtime'''
        return self.name

    def get_cache_name(self):
        return '_%s_cache' % self.name

    def id(self, obj):
        '''Field id for object *obj*, if applicable. Default is ``None``.'''
        return None

    def get_default(self):
        "Returns the default value for this field."
        if hasattr(self.default,'__call__'):
            return self.default()
        else:
            return self.default

    def __deepcopy__(self, memodict):
        '''Nothing to deepcopy here'''
        field = copy(self)
        field.name = None
        field.model = None
        field.meta = None
        return field

    def filter(self, session, name, value):
        pass

    def get_sorting(self, name, errorClass):
        raise errorClass('Cannot use nested sorting on field {0}'.format(self))

    def todelete(self):
        return False

    ############################################################################
    ##    FIELD CONVERTERS
    ############################################################################

    def to_python(self, value):
        """Converts the input value into the expected Python
data type, raising :class:`stdnet.FieldValueError` if the data
can't be converted.
Returns the converted value. Subclasses should override this."""
        return value

    def serialize(self, value):
        '''It returns a representation of *value* to store in the database.
If an error occurs it raises :class:`stdnet.exceptions.FieldValueError`'''
        return self.to_python(value)

    def json_serialize(self, value):
        '''Return a representation of this field which is compatible with
 JSON.'''
        return None

    def scorefun(self, value):
        '''Function which evaluate a score from the field value. Used by
the ordering alorithm'''
        return self.to_python(value)
    
    def dumps(self, value, lookup=None):
        return self.serialize(value)

    ############################################################################
    ##    TOOLS
    ############################################################################

    def _handle_extras(self, **extras):
        '''Callback to hadle extra arguments during initialization.'''
        self.error_extras(extras)


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
    def to_python(self, value):
        if hasattr(value, '_meta'):
            return value.pkvalue()
        else:
            return value
    json_serialize = to_python


class BooleanField(AtomField):
    '''A boolean :class:`AtomField`'''
    type = 'bool'
    internal_type = 'numeric'
    python_type = bool
    default = False

    def __init__(self, required = False, **kwargs):
        super(BooleanField,self).__init__(required = required,**kwargs)

    @field_value_error
    def to_python(self, value):
        if value in NONE_EMPTY:
            return self.get_default()
        else:
            return self.python_type(int(value))

    def serialize(self, value):
        return 1 if value else 0
    scorefun = serialize


class IntegerField(AtomField):
    '''An integer :class:`AtomField`.'''
    type = 'integer'
    internal_type = 'numeric'
    python_type = int
    #default = 0

    @field_value_error
    def to_python(self, value):
        value = super(IntegerField,self).to_python(value)
        if value in NONE_EMPTY:
            return self.get_default()
        else:
            return self.python_type(value)


class AutoField(IntegerField):
    '''An :class:`IntegerField` that automatically increments.
You usually won't need to use this directly;
a ``primary_key`` field  of this type, named ``id``,
will automatically be added to your model
if you don't specify otherwise.
    '''
    type = 'auto'


class FloatField(IntegerField):
    '''An floating point :class:`AtomField`. By default
its :attr:`Field.index` is set to ``False``.
    '''
    type = 'float'
    internal_type = 'numeric'
    index = False
    python_type = float


class DateField(AtomField):
    '''An :class:`AtomField` represented in Python by
a :class:`datetime.date` instance.'''
    type = 'date'
    internal_type = 'numeric'
    python_type = date
    ordered = True
    default = None

    @field_value_error
    def to_python(self, value):
        if value not in NONE_EMPTY:
            if isinstance(value,date):
                if isinstance(value,datetime):
                    value = value.date()
            else:
                value = timestamp2date(float(value)).date()
            return value
        else:
            return self.get_default()

    @field_value_error
    def serialize(self, value):
        if value not in NONE_EMPTY:
            if isinstance(value, date):
                value = date2timestamp(value)
            else:
                raise FieldValueError('Field %s is not a valid date' % self)
        return value
    scorefun = serialize
    json_serialize = serialize


class DateTimeField(DateField):
    '''A date :class:`AtomField` represented in Python by
a :class:`datetime.datetime` instance.'''
    type = 'datetime'
    python_type = datetime
    index = False

    @field_value_error
    def to_python(self, value):
        if value not in NONE_EMPTY:
            if isinstance(value,date):
                if not isinstance(value,datetime):
                    value = datetime(value.year,value.month,value.day)
            else:
                value = timestamp2date(float(value))
            return value
        else:
            return self.get_default()


class SymbolField(AtomField):
    '''An :class:`AtomField` which contains a ``symbol``.
A symbol holds a unicode string as a single unit.
A symbol is irreducible, and are often used to hold names, codes
or other entities. They are indexes by default.'''
    type = 'text'
    python_type = string_type
    internal_type = 'text'
    charset = 'utf-8'
    default = ''

    def get_encoder(self, params):
        return encoders.Default(self.charset)

    @field_value_error
    def to_python(self, value):
        value = super(SymbolField,self).to_python(value)
        if value is not None:
            return self.encoder.loads(value)
        else:
            return self.get_default()

    def scorefun(self, value):
        raise FieldValueError('Could not obtain score')


class CharField(SymbolField):
    '''A text :class:`SymbolField` which is never an index.
It contains unicode and by default and :attr:`Field.required`
is set to ``False``.'''
    def __init__(self, *args, **kwargs):
        kwargs['index'] = False
        kwargs['unique'] = False
        kwargs['primary_key'] = False
        self.max_length = kwargs.pop('max_length',None) # not used for now
        required = kwargs.get('required',None)
        if required is None:
            kwargs['required'] = False
        super(CharField,self).__init__(*args, **kwargs)


class ByteField(CharField):
    '''A :class:`CharField` which contains binary data.
In python this is converted to `bytes`.'''
    type = 'bytes'
    internal_type = 'bytes'
    python_type = bytes
    default = b''

    @field_value_error
    def to_python(self, value):
        if value is not None:
            return self.encoder.loads(value)
        else:
            return self.get_default()

    @field_value_error
    def json_serialize(self, value):
        if value is not None:
            return b64encode(self.serialize(value)).decode(self.charset)

    def get_encoder(self, params):
        return encoders.Bytes(self.charset)


class PickleObjectField(ByteField):
    '''A field which implements automatic conversion to and form a pickable
python object.
This field is python specific and therefore not of much use
if accessed from external programs. Consider the :class:`ForeignKey`
or :class:`JSONField` fields as more general alternatives.

.. note:: The best way to use this field is when its :class:`Field.as_cache`
          attribute is ``True``.
'''
    type = 'object'
    default = None

    def serialize(self, value):
        if value is not None:
            return self.encoder.dumps(value)

    def get_encoder(self, params):
        return encoders.PythonPickle(protocol = 2)


class ForeignKey(Field):
    '''A field defining a :ref:`one-to-many <one-to-many>` objects relationship.
Requires a positional argument: the class to which the model is related.
For example::

    class Folder(odm.StdModel):
        name = odm.SymobolField()

    class File(odm.StdModel):
        folder = odm.ForeignKey(Folder, related_name = 'files')

To create a recursive relationship, an object that has a many-to-one
relationship with itself use::

    odm.ForeignKey('self')

Behind the scenes, stdnet appends "_id" to the field name to create
its field name in the back-end data-server. In the above example,
the database field for the ``File`` model will have a ``folder_id`` field.

It accepts **related_name** as extra argument. It is the name to use for
the relation from the related object back to self.
'''
    type = 'related object'
    internal_type = 'numeric'
    python_type = int
    proxy_class = related.LazyForeignKey
    related_manager_class = related.One2ManyRelatedManager

    def __init__(self, model, related_name=None, related_manager_class=None,
                 **kwargs):
        if related_manager_class:
            self.related_manager_class = related_manager_class
        super(ForeignKey,self).__init__(**kwargs)
        if not model:
            raise FieldError('Model not specified')
        self.relmodel = model
        self.related_name = related_name

    def register_with_related_model(self):
        # add the RelatedManager proxy to the model holding the field
        setattr(self.model, self.name, self.proxy_class(self))
        setattr(self.model, self.get_query_attname(),
                related.LazyForeignQuery(self))
        related.load_relmodel(self, self._set_relmodel)

    def _set_relmodel(self, relmodel):
        self.relmodel = relmodel
        meta  = self.relmodel._meta
        related_name = self.related_name or '%s_set' % self.model._meta.name
        if related_name not in meta.related and related_name\
                                                 not in meta.dfields:
            self.related_name = related_name
            self._register_with_related_model()
        else:
            raise FieldError('Duplicated related name "{0}"\
 in model "{1}" and field {2}'.format(related_name,meta,self))

    def _register_with_related_model(self):
        manager = self.related_manager_class(self)
        setattr(self.relmodel, self.related_name, manager)
        self.relmodel._meta.related[self.related_name] = manager
        self.relmodel_manager = manager

    def get_attname(self):
        return '%s_id' % self.name

    def get_query_attname(self):
        return '%s_query' % self.name

    def register_with_model(self, name, model):
        super(ForeignKey,self).register_with_model(name, model)
        if not model._meta.abstract:
            self.register_with_related_model()

    def scorefun(self, value):
        if isinstance(value, self.relmodel):
            return value.scorefun()
        else:
            raise FieldValueError('cannot evaluate score of {0}'.format(value))

    @field_value_error
    def to_python(self, value):
        if isinstance(value,self.relmodel):
            return value.id
        else:
            return self.relmodel._meta.pk_to_python(value)
    json_serialize = to_python

    def filter(self, session, name, value):
        fname = name.split('__')[0]
        if fname in self.relmodel._meta.dfields:
            return session.query(self.relmodel, fargs = {name: value})

    def get_sorting(self, name, errorClass):
        return self.relmodel._meta.get_sorting(name, errorClass)


class JSONField(CharField):
    '''A JSON field which implements automatic conversion to
and from an object and a JSON string. It is the responsability of the
user making sure the object is JSON serializable.

There are few extra parameters which can be used to customize the
behaviour and how the field is stored in the back-end server.

:parameter encoder_class: The JSON class used for encoding.

    Default: :class:`stdnet.utils.jsontools.JSONDateDecimalEncoder`.

:parameter decoder_hook: A JSON decoder function.

    Default: :class:`stdnet.utils.jsontools.date_decimal_hook`.

:parameter as_string: Set the :attr:`as_string` attribute.

    Default ``True``.

.. attribute:: as_string

    A boolean indicating if data should be serialized
    into a single JSON string or it should be used to create several
    fields prefixed with the field name and the double underscore ``__``.

    Default ``True``.

    Effectively, a :class:`JSONField` with ``as_string`` attribute set to
    ``False`` is a multifield, in the sense that it generates several
    field-value pairs. For example, lets consider the following::

        class MyModel(odm.StdModel):
            name = odm.SymbolField()
            data = odm.JSONField(as_string=False)

    And::

        >>> m = MyModel(name = 'bla',
                        data = {'pv': {'': 0.5, 'mean': 1, 'std': 3.5}})
        >>> m.cleaned_data
        {'name': 'bla', 'data__pv': 0.5, 'data__pv__mean': '1',\
 'data__pv__std': '3.5', 'data': '""'}
        >>>

    The reason for setting ``as_string`` to ``False`` is to allow
    the :class:`JSONField` to define several fields at runtime,
    without introducing new :class:`Field` in your model class.
    These fields behave exactly like standard fields and therefore you
    can, for example, sort queries with respect to them::

        >>> MyModel.objects.query().sort_by('data__pv__std')
        >>> MyModel.objects.query().sort_by('-data__pv')

    which can be rather useful feature.
'''
    type = 'json object'
    internal_type = 'serialized'
    default = None

    def get_encoder(self, params):
        self.as_string = params.pop('as_string',True)
        if not self.as_string:
            self.default = self.default if isinstance(self.default,dict) else {}
        return encoders.Json(
                charset = self.charset,
                json_encoder = params.pop('encoder_class',DefaultJSONEncoder),
                object_hook = params.pop('decoder_hook',DefaultJSONHook))

    @field_value_error
    def to_python(self, value):
        if value is None:
            return self.get_default()
        try:
            return self.encoder.loads(value)
        except TypeError:
            return value

    @field_value_error
    def serialize(self, value):
        if self.as_string:
            # dump as a string
            return self.dumps(value)
        else:
            # unwind as a dictionary
            value = dict(dict_flat_generator(value,
                                             attname=self.attname,
                                             dumps=self.dumps,
                                             error=FieldValueError))
            # If the dictionary is empty we modify so that
            # an update is possible.
            if not value:
                value = {self.attname: self.dumps(None)}
            elif value.get(self.attname, None) is None:
                # TODO Better implementation of this is a ack!
                # set the root value to an empty string to distinguish
                # from None.
                value[self.attname] = self.dumps('')
            return value

    def dumps(self, value, lookup=None):
        if lookup:
            value = range_lookups[lookup](value)
        try:
            return self.encoder.dumps(value)
        except TypeError as e:
            raise FieldValueError(str(e))

    def value_from_data(self, instance, data):
        if self.as_string:
            return data.pop(self.attname, None)
        else:
            return flat_to_nested(data, instance=instance,
                                  attname=self.attname,
                                  loads=self.encoder.loads)

    def get_sorting(self, name, errorClass):
        pass


class ModelField(SymbolField):
    '''A filed which can be used to store the model classes (not only
:class:`StdModel` models). If a class has a attribute ``_meta``
with a unique hash attribute ``hash`` and it is
registered in the model hash table, it can be used.'''
    type = 'model'
    internal_type = 'text'

    def json_serialize(self, value):
        return self.index_value(value)

    @field_value_error
    def to_python(self, value):
        if value and not hasattr(value,'_meta'):
            value = self.encoder.loads(value)
            return get_model_from_hash(value)
        else:
            return value

    @field_value_error
    def index_value(self, value):
        if value is not None:
            v = get_hash_from_model(value)
            if v is None:
                value = self.to_python(value)
                return get_hash_from_model(value)
            else:
                return v

    def serialize(self, value):
        value = self.index_value(value)
        if value is not None:
            return self.encoder.dumps(value)


class ManyToManyField(Field):
    '''A many-to-many relationship. Like :class:`ForeignKey`, it accepts
**related_name** as extra argument.

.. attribute:: related_name

    Optional name to use for the relation from the related object
    back to ``self``.

.. attribute:: through

    Optional :class:`StdModel` to use for creating the many-to-many
    relationship.

For example::

    class Group(odm.StdModel):
        name = odm.SymbolField(unique = True)

    class User(odm.StdModel):
        name = odm.SymbolField(unique = True)
        groups = odm.ManyToManyField(Group, related_name = 'users')

To use it::

    >>> g = Group(name = 'developers').save()
    >>> g.users.add(User(name = 'john').save())
    >>> u.users.add(User(name = 'mark').save())

and to remove::

    >>> u.following.remove(User.objects.get(name = 'john))


Under the hood, a :class:`ManyToManyField` creates a new model *model_name*.
If not provided, the the name will be constructed from the field name
and the model holding the field. In the example above it would be
*group_user*.
This model contains two :class:`ForeignKeys`, one to model holding the
:class:`ManyToManyField` and the other to the *related_model*.
'''
    def __init__(self, model, through = None, related_name = None, **kwargs):
        self.through = through
        self.relmodel = model
        self.related_name = related_name
        super(ManyToManyField,self).__init__(model,**kwargs)

    def register_with_model(self, name, model):
        super(ManyToManyField,self).register_with_model(name, model)
        if not model._meta.abstract:
            related.load_relmodel(self, self._set_relmodel)

    def _set_relmodel(self, relmodel):
        self.relmodel = relmodel
        if not self.related_name:
            self.related_name = '%s_set' % self.model._meta.name
        related.Many2ManyThroughModel(self)

    def get_attname(self):
        return None

    def todelete(self):
        return False

    def add_to_fields(self):
        #A many to many field is a dummy field. All it does it provides a proxy
        #for the through model.
        self.meta.dfields.pop(self.name)


class CompositeIdField(SymbolField):
    '''This field can be used when an instance of a model is uniquely
identified by a combination of two or more :class:`Field` in the model
itself.'''
    type = 'composite'
    def __init__(self, *fields, **kwargs):
        kwargs['primary_key'] = True
        super(CompositeIdField,self).__init__(**kwargs)
        self.fields = fields

