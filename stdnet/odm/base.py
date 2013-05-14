'''Defines Metaclasses and Base classes for stdnet Models.'''
import sys
from copy import copy, deepcopy
import hashlib
import weakref

from stdnet.utils.exceptions import *
from stdnet.utils import zip, to_string, UnicodeMixin, unique_tuple

from . import signals
from .globals import hashmodel, JSPLITTER, get_model_from_hash, orderinginfo,\
                     range_lookup_info
from .fields import Field, AutoIdField


__all__ = ['ModelMeta',
           'Metaclass',
           'Model',
           'ModelBase',
           'ModelState',
           'create_model',
           'autoincrement',
           'ModelType', # Metaclass for all stdnet ModelBase classes
           'StdNetType']


def get_fields(bases, attrs):
    fields = {}
    for base in bases:
        if hasattr(base, '_meta'):
            fields.update(deepcopy(base._meta.dfields))

    for name,field in list(attrs.items()):
        if isinstance(field,Field):
            fields[name] = attrs.pop(name)

    return fields


def make_app_label(new_class, app_label=None):
    if app_label is None:
        model_module = sys.modules[new_class.__module__]
        try:
            bits = model_module.__name__.split('.')
            app_label = bits.pop()
            if app_label == 'models':
                app_label = bits.pop()
        except:
            app_label = ''
    return app_label


class ModelMeta(object):
    '''A class for storing meta data of a :class:`Model` class. It is the
base class of :class:`Metaclass`.

.. attribute:: abstract

    If ``True``, it represents an abstract model and no database elements
    are created.
    
.. attribute:: model

    :class:`Model` for which this class is the database metadata container.
    
.. attribute:: name

    Usually it is the :class:`Model` class name in lower-case, but it
    can be customised.

.. attribute:: app_label

    Unless specified it is the name of the directory or file
    (if at top level) containing the :class:`Model` definition. It can be
    customised.
    
.. attribute:: modelkey

    The modelkey which is by default given by ``app_label.name``.
'''
    def __init__(self, model, app_label=None, modelkey=None, abstract=False,
                  name=None, register=True, **kwargs):
        self.abstract = abstract
        self.model = model
        self.model._meta = self
        self.app_label = make_app_label(model, app_label)
        self.name = (name or model.__name__).lower()
        if not modelkey:
            if self.app_label:
                modelkey = '{0}.{1}'.format(self.app_label, self.name)
            else:
                modelkey = self.name
        self.modelkey = modelkey
        if not abstract and register:
            hashmodel(model)

    @property
    def type(self):
        '''Model type, either ``structure`` or ``object``.'''
        return self.model._model_type
    
    def pkname(self):
        '''The name of the primary key.'''
        return 'id'
    
    def pk_to_python(self, value, backend):
        '''Convert the primary key ``value`` to a valid python representation.'''
        return value

    def make_object(self, state=None, backend=None):
        '''Create a new instance of :attr:`model` from a *state* tuple.'''
        model = self.model
        obj = model.__new__(model)
        self.load_state(obj, state, backend)
        return obj
        
    def load_state(self, obj, state=None, backend=None):
        if state:
            id, loadedfields, data = state
            field = self.pk
            pkvalue = field.to_python(id, backend)
            setattr(obj, field.attname, pkvalue)
            if loadedfields is not None:
                loadedfields = tuple(loadedfields)
            obj._loadedfields = loadedfields
            fields = self.dfields
            for field in obj.loadedfields():
                value = field.value_from_data(obj, data)
                setattr(obj, field.attname, field.to_python(value, backend))
            pkname = self.pkname()
            if backend or ('__dbdata__' in data and\
                            data['__dbdata__'][pkname] == pkvalue):
                obj._dbdata[self.pkname()] = pkvalue

    def __repr__(self):
        return self.modelkey

    def __str__(self):
        return self.__repr__()


class Metaclass(ModelMeta):
    '''A :class:`ModelMeta` for storing information which maps a
:class:`StdModel` into its remote counterpart in the
:class:`stdnet.BackendDataServer`.
An instance is initiated by the :mod:`stdnet.odm` when a :class:`StdModel`
class is created.

To override default behaviour you can specify the ``Meta`` class as an inner
class of :class:`StdModel` in the following way::

    from datetime import datetime
    from stdnet import odm

    class MyModel(odm.StdModel):
        timestamp = odm.DateTimeField(default = datetime.now)
        ...

        class Meta:
            ordering = '-timestamp'
            name = 'custom'


:parameter abstract: Check the :attr:`ModelMeta.abstract` attribute.
:parameter ordering: Check the :attr:`ordering` attribute.
:parameter app_label: Check the :attr:`ModelMeta.app_label` attribute.
:parameter name: Check the :attr:`ModelMeta.name` attribute.
:parameter modelkey: Check the :attr:`ModelMeta.modelkey` attribute.

**Attributes and methods**:

This is the list of attributes and methods available. All attributes,
but the ones mantioned above, are initialized by the object relational
mapper.

.. attribute:: ordering

    Optional name of a :class:`Field` in the :attr:`model`.
    If provided, model indices will be sorted with respect to the value of the
    specified field. It can also be a :class:`autoincrement` instance.
    Check the :ref:`sorting <sorting>` documentation for more details.

    Default: ``None``.ma

.. attribute:: dfields

    dictionary of :class:`Field` instances. It does not include
    :class:`StructureField`.

.. attribute:: fields

    list of all :class:`Field` instances.

.. attribute:: indices

    List of :class:`Field` which are indices (:attr:`Field.index` attribute
    set to ``True``).

.. attribute:: pk

    The :class:`Field` representing the primary key.
    
.. attribute:: related

    Dictionary of :class:`RelatedManager` for the :attr:`model`. It is
    created at runtime by the object data mapper.
    
.. attribute:: manytomany

    List of :class:`ManyToManyField` names for the :attr:`model`. This
    information is useful during registration.
'''
    connection_string = None

    def __init__(self, model, fields, ordering=None, **kwargs):
        super(Metaclass, self).__init__(model, **kwargs)
        self.fields = []
        self.scalarfields = []
        self.indices = []
        self.multifields = []
        self.dfields = {}
        self.timeout = 0
        self.related = {}
        self.manytomany = []
        # Check if PK field exists
        pk = None
        pkname = 'id'
        for name in fields:
            field = fields[name]
            if field.primary_key:
                if pk is not None:
                    raise FieldError("Primary key already available %s." % name)
                pk = field
                pkname = name
        if pk is None:
            # ID field not available, create one
            pk = AutoIdField(primary_key=True)
        fields.pop(pkname, None)
        for name, field in fields.items():
            field.register_with_model(name, model)
        pk.register_with_model(pkname, model)
        self.ordering = None
        if ordering:
            self.ordering = self.get_sorting(ordering, ImproperlyConfigured)
    
    def pkname(self):
        '''Primary key name. A shortcut for ``self.pk.name``.'''
        return self.pk.name

    def pk_to_python(self, value, backend):
        return self.pk.to_python(value, backend)
    
    def is_valid(self, instance):
        '''Perform validation for *instance* and stores serialized data,
indexes and errors into local cache.
Return ``True`` if the instance is ready to be saved to database.'''
        dbdata = instance._dbdata
        data = dbdata['cleaned_data'] = {}
        errors = dbdata['errors'] = {}
        #Loop over scalar fields first
        for field, value in instance.fieldvalue_pairs():
            name = field.attname
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
                        data.update(svalue)
                    else:
                        if svalue is not None:
                            data[name] = svalue
        return len(errors) == 0

    def get_sorting(self, sortby, errorClass=None):
        s = None
        desc = False
        if isinstance(sortby, autoincrement):
            f = self.pk
            return orderinginfo(sortby, f, desc, self.model, None, True)
        elif sortby.startswith('-'):
            desc = True
            sortby = sortby[1:]
        if sortby == self.pkname():
            f = self.pk
            return orderinginfo(f.attname, f, desc, self.model, None, False)
        else:
            if sortby in self.dfields:
                f = self.dfields[sortby]
                return orderinginfo(f.attname, f, desc, self.model, None, False)
            sortbys = sortby.split(JSPLITTER)
            s0 = sortbys[0]
            if len(sortbys) > 1 and s0 in self.dfields:
                f = self.dfields[s0]
                nested = f.get_sorting(JSPLITTER.join(sortbys[1:]),errorClass)
                if nested:
                    sortby = f.attname
                return orderinginfo(sortby, f, desc, self.model, nested, False)
        errorClass = errorClass or ValueError
        raise errorClass('Cannot Order by attribute "{0}".\
 It is not a scalar field.'.format(sortby))

    def backend_fields(self, fields):
        '''Return a two elements tuple containing a list
of fields names and a list of field attribute names.'''
        dfields = self.dfields
        processed = set()
        names = []
        atts = []
        pkname = self.pkname()
        for name in fields:
            if name == pkname or name in processed:
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
                    if field.type in ('json object', 'related object'):
                        processed.add(name)
                        names.append(name)
                        atts.append(name)
        return names, atts

    def as_dict(self):
        '''Model metadata in a dictionary'''
        pk = self.pk
        id_type = 3
        id_fields = None
        if pk.type == 'auto':
            id_type = 1
        return {'id_name': pk.name,
                'id_type': id_type,
                'sorted': bool(self.ordering),
                'autoincr': self.ordering and self.ordering.auto,
                'multi_fields': [field.name for field in self.multifields],
                'indices': dict(((idx.attname, idx.unique)\
                                for idx in self.indices))}

class autoincrement(object):
    '''An :class:`autoincrement` is used in a :class:`StdModel` Meta
class to specify a model with :ref:`incremental sorting <incremental-sorting>`.

.. attribute:: incrby

    The amount to increment the score by when a duplicate element is saved.

    Default: 1.

For example, the :class:`stdnet.apps.searchengine.Word` model is defined as::

    class Word(odm.StdModel):
        id = odm.SymbolField(primary_key = True)

        class Meta:
            ordering = -autoincrement()

This means every time we save a new instance of Word, and that instance has
an id already available, the score of that word is incremented by the
:attr:`incrby` attribute.

'''
    def __init__(self, incrby=1, desc=False):
        self.incrby = incrby
        self._asce = -1 if desc else 1

    def __neg__(self):
        c = copy(self)
        c._asce *= -1
        return c

    @property
    def desc(self):
        return True if self._asce == -1 else False

    def __repr__(self):
        return ('' if self._asce == 1 else '-') + '{0}({1})'\
            .format(self.__class__.__name__,self.incrby)

    def __str__(self):
        return self.__repr__()


class ModelType(type):
    '''Model metaclass'''
    def __new__(cls, name, bases, attrs):
        if attrs.pop('is_base_class', False):
            return super(ModelType, cls).__new__(cls, name, bases, attrs)
        return cls.make(name, bases, attrs, attrs.pop('Meta', None))

    @classmethod
    def make(cls, name, bases, attrs, meta):
        register = attrs.pop('register', True)
        attributes = attrs.pop('attributes', None)
        new_class = type.__new__(cls, name, bases, attrs)
        ModelMeta(new_class, register=register)
        if attributes is not None:
            new_class._meta.attributes = attributes 
        return new_class


class StdNetType(ModelType):
    '''metaclass for StdModel'''
    @classmethod
    def make(cls, name, bases, attrs, meta):
        kwargs = meta.__dict__ if meta else {}
        # remove and build field list
        fields = get_fields(bases, attrs)
        # create the new class
        new_class = type.__new__(cls, name, bases, attrs)
        Metaclass(new_class, fields, **kwargs)
        signals.class_prepared.send(sender=new_class)
        return new_class


class ModelState(object):
    '''The database state of a :class:`Model`.'''
    def __init__(self, instance, iid=None, action=None):
        self._action = action or 'add'
        self.deleted = False
        self.score = 0
        dbdata = instance._dbdata
        pkname = instance._meta.pkname()
        pkvalue = iid or getattr(instance, pkname, None)
        if pkvalue and pkname in dbdata:
            if self._action == 'add':
                self._action = instance.get_state_action()
        elif not pkvalue:
            self._action = 'add'
            pkvalue = 'new.{0}'.format(id(instance))
        self._iid = pkvalue

    @property
    def action(self):
        '''Action to be performed by the backend server when committing
changes to the instance of :class:`Model` for which this is a state.'''
        return self._action
    
    @property
    def persistent(self):
        '''``True`` if the instance is persistent in the backend server.'''
        return self._action != 'add'

    @property
    def iid(self):
        '''Instance primary key or a temporary key if not yet available.'''
        return self._iid

    def __repr__(self):
        return '%s%s' % (self.iid, ' deleted' if self.deleted else '')
    __str__ = __repr__


class Model(UnicodeMixin):
    '''This is the base class for both :class:`StdModel` and :class:`Structure`
classes. It implements the :attr:`uuid` attribute which provides the universal
unique identifier for an instance of a model.

.. attribute:: _meta

    A class attribute which is an instance of :class:`Metaclass`, it
    containes all the information needed by a :class:`stdnet.BackendDataServer`.
    
.. attribute:: session

    The :class:`Session` which loaded the instance. Only available,
    when the instance has been loaded from a :class:`stdnet.BackendDataServer`
    via a :ref:`query operation <tutorial-query>`.
'''
    _model_type = None
    DoesNotExist = ObjectNotFound
    '''Exception raised when an instance of a model does not exist.'''
    DoesNotValidate = ObjectNotValidated
    '''Exception raised when an instance of a model does not validate. Usually
raised when trying to save an invalid instance.'''

    def __new__(cls, *args, **kwargs):
        o = super(Model, cls).__new__(cls)
        pkname = cls._meta.pkname()
        setattr(o, pkname, kwargs.pop(pkname, None))
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
        return hash(self.get_uuid(self.get_state().iid))

    def get_state(self, **kwargs):
        '''Return the current :class:`ModelState` for this :class:`Model`.
If ``kwargs`` parameters are passed a new :class:`ModelState` is created,
otherwise it returns the cached value.'''
        if 'state' not in self._dbdata or kwargs:
            self._dbdata['state'] = ModelState(self, **kwargs)
        return self._dbdata['state']

    def pkvalue(self):
        '''Value of primary key'''
        return self._meta.pk.get_value(self)

    @classmethod
    def get_uuid(cls, id):
        return '%s.%s' % (cls._meta.hash, id)

    @property
    def uuid(self):
        '''Universally unique identifier for an instance of a :class:`Model`.'''
        if not self.id:
            raise self.DoesNotExist(\
                    'Object not saved. Cannot obtain universally unique id')
        return self.get_uuid(self.id)

    def __get_session(self):
        return self._dbdata.get('session')
    def __set_session(self, session):
        self._dbdata['session'] = session
    session = property(__get_session, __set_session)
    
    def get_attr_value(self, name):
        '''Provided for compatibility with :meth:`StdModel.get_attr_value`.
For this class it simply get the attribute at name::
    
    return getattr(self, name)
'''
        return getattr(self, name)
    
    def get_state_action(self):
        return 'update'


ModelBase = ModelType('ModelBase', (Model,), {'is_base_class': True})


def raise_kwargs(model, kwargs):
    if kwargs:
        keys = ', '.join(kwargs)
        if len(kwargs) > 1:
            keys += ' are'
        else:
            keys += ' is an'
        raise ValueError("%s invalid keyword for %s." % (keys, model._meta))


class LocalModelBase(Model):
    def __init__(self, *args, **kwargs):
        attributes = self._meta.attributes
        if args:
            N = len(args)
            if N > len(attributes):
                raise ValueError('Too many attributes')
            attrs, attributes = attributes[:N], attributes[N:]
            for name, value in zip(attrs, args):
                setattr(self, name, value)
        for name in attributes:
            setattr(self, name, kwargs.pop(name, None))
        raise_kwargs(self, kwargs)
        

def create_model(name, *attributes, **params):
    '''Create a local model class'''
    params['attributes'] = unique_tuple(attributes)
    params['register'] = False
    return ModelType(name, (LocalModelBase,), params)
