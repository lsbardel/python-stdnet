'''Defines Metaclasses and Base classes for stdnet Models.'''
import sys
from copy import copy, deepcopy
from inspect import isclass

from stdnet.utils.exceptions import *
from stdnet.utils import UnicodeMixin, unique_tuple
from stdnet.utils.structures import OrderedDict

from .globals import hashmodel, JSPLITTER, orderinginfo
from .fields import Field, AutoIdField
from .related import class_prepared


__all__ = ['ModelMeta', 'Model', 'ModelBase', 'ModelState',
           'autoincrement', 'ModelType']


def get_fields(bases, attrs):
    #
    fields = []
    for name, field in list(attrs.items()):
        if isinstance(field, Field):
            fields.append((name, attrs.pop(name)))
    #
    fields = sorted(fields, key=lambda x: x[1].creation_counter)
    #
    for base in bases:
        if hasattr(base, '_meta'):
            fields = list((name, deepcopy(field)) for name, field\
                           in base._meta.dfields.items()) + fields
    #
    return OrderedDict(fields)


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
    '''A class for storing meta data for a :class:`Model` class.
To override default behaviour you can specify the ``Meta`` class as an inner
class of :class:`Model` in the following way::

    from datetime import datetime
    from stdnet import odm

    class MyModel(odm.StdModel):
        timestamp = odm.DateTimeField(default = datetime.now)
        ...

        class Meta:
            ordering = '-timestamp'
            name = 'custom'


:parameter register: if ``True`` (default), this :class:`ModelMeta` is
    registered in the global models hashtable.
:parameter abstract: Check the :attr:`abstract` attribute.
:parameter ordering: Check the :attr:`ordering` attribute.
:parameter app_label: Check the :attr:`app_label` attribute.
:parameter name: Check the :attr:`name` attribute.
:parameter modelkey: Check the :attr:`modelkey` attribute.
:parameter attributes: Check the :attr:`attributes` attribute.

This is the list of attributes and methods available. All attributes,
but the ones mantioned above, are initialized by the object relational
mapper.

.. attribute:: abstract

    If ``True``, This is an abstract Meta class.

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
    
.. attribute:: ordering

    Optional name of a :class:`Field` in the :attr:`model`.
    If provided, model indices will be sorted with respect to the value of the
    specified field. It can also be a :class:`autoincrement` instance.
    Check the :ref:`sorting <sorting>` documentation for more details.

    Default: ``None``.

.. attribute:: dfields

    dictionary of :class:`Field` instances.

.. attribute:: fields

    list of all :class:`Field` instances.

.. attribute:: scalarfields

    Ordered list of all :class:`Field` which are not :class:`StructureField`.
    The order is the same as in the :class:`Model` definition. The :attr:`pk`
    field is not included.
    
.. attribute:: indices

    List of :class:`Field` which are indices (:attr:`Field.index` attribute
    set to ``True``).

.. attribute:: pk

    The :class:`Field` representing the primary key.
    
.. attribute:: related

    Dictionary of :class:`related.RelatedManager` for the :attr:`model`. It is
    created at runtime by the object data mapper.
    
.. attribute:: manytomany

    List of :class:`ManyToManyField` names for the :attr:`model`. This
    information is useful during registration.
    
.. attribute:: attributes

    Additional attributes for :attr:`model`.
'''
    def __init__(self, model, fields, app_label=None, modelkey=None,
                 name=None, register=True, pkname=None, ordering=None,
                 attributes=None, abstract=False, **kwargs):
        self.model = model
        self.abstract = abstract
        self.attributes = unique_tuple(attributes or ())
        self.dfields = {}
        self.fields = []
        self.scalarfields = []
        self.indices = []
        self.multifields = []
        self.related = {}
        self.manytomany = []
        self.model._meta = self
        self.app_label = make_app_label(model, app_label)
        self.name = (name or model.__name__).lower()
        if not modelkey:
            if self.app_label:
                modelkey = '{0}.{1}'.format(self.app_label, self.name)
            else:
                modelkey = self.name
        self.modelkey = modelkey
        if not self.abstract and register:
            hashmodel(model)
        #
        # Check if PK field exists
        pk = None
        pkname = pkname or 'id'
        for name in fields:
            field = fields[name]
            if field.primary_key:
                if pk is not None:
                    raise FieldError("Primary key already available %s." % name)
                pk = field
                pkname = name
        if pk is None and not self.abstract:
            # ID field not available, create one
            pk = AutoIdField(primary_key=True)
        fields.pop(pkname, None)
        for name, field in fields.items():
            field.register_with_model(name, model)
        if pk is not None:
            pk.register_with_model(pkname, model)
        self.ordering = None
        if ordering:
            self.ordering = self.get_sorting(ordering, ImproperlyConfigured)

    @property
    def type(self):
        '''Model type, either ``structure`` or ``object``.'''
        return self.model._model_type
    
    def make_object(self, state=None, backend=None):
        '''Create a new instance of :attr:`model` from a *state* tuple.'''
        model = self.model
        obj = model.__new__(model)
        self.load_state(obj, state, backend)
        return obj
        
    def load_state(self, obj, state=None, backend=None):
        if state:
            pkvalue, loadedfields, data = state
            pk = self.pk
            pkvalue = pk.to_python(pkvalue, backend)
            setattr(obj, pk.attname, pkvalue)
            if loadedfields is not None:
                loadedfields = tuple(loadedfields)
            obj._loadedfields = loadedfields
            for field in obj.loadedfields():
                value = field.value_from_data(obj, data)
                setattr(obj, field.attname, field.to_python(value, backend))
            if backend or ('__dbdata__' in data and\
                            data['__dbdata__'][pk.name] == pkvalue):
                obj._dbdata[pk.name] = pkvalue

    def __repr__(self):
        return self.modelkey

    def __str__(self):
        return self.__repr__()
    
    def pkname(self):
        '''Primary key name. A shortcut for ``self.pk.name``.'''
        return self.pk.name

    def pk_to_python(self, value, backend):
        '''Convert the primary key ``value`` to a valid python representation.'''
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
            try:
                svalue = field.set_get_value(instance, value)
            except Exception as e:
                errors[name] = str(e)
            else:
                if (svalue is None or svalue is '') and field.required:
                    errors[name] = "Field '{0}' is required for '{1}'."\
                                    .format(name, self)
                else:
                    if isinstance(svalue, dict):
                        data.update(svalue)
                    elif svalue is not None:
                        data[name] = svalue
        return len(errors) == 0

    def get_sorting(self, sortby, errorClass=None):
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
        raise errorClass('"%s" cannot order by attribute "%s". It is not a '
                         'scalar field.' % (self, sortby))

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
        meta = attrs.pop('Meta', None)
        if isclass(meta):
            meta = dict(((k, v) for k, v in meta.__dict__.items()\
                          if not k.startswith('__')))
        else:
            meta = meta or {}
        cls.extend_meta(meta, attrs)
        fields = get_fields(bases, attrs)
        new_class = super(ModelType, cls).__new__(cls, name, bases, attrs)
        ModelMeta(new_class, fields, **meta)
        class_prepared.send(sender=new_class)
        return new_class
    
    @classmethod
    def extend_meta(cls, meta, attrs):
        for name in ('register', 'abstract', 'attributes'):
            if name in attrs:
                meta[name] = attrs.pop(name)


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

    A class attribute which is an instance of :class:`ModelMeta`, it
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
        pkname = cls._meta.pk.name
        setattr(o, pkname, kwargs.get(pkname))
        o._dbdata = {}
        return o

    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return self.pkvalue() == other.pkvalue()
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
    def get_uuid(cls, pk):
        return '%s.%s' % (cls._meta.hash, pk)

    @property
    def uuid(self):
        '''Universally unique identifier for an instance of a :class:`Model`.'''
        pk = self.pkvalue()
        if not pk:
            raise self.DoesNotExist(\
                    'Object not saved. Cannot obtain universally unique id')
        return self.get_uuid(pk)

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
    
    def save(self):
        '''Save the model by adding it to the :attr:`session`. If the
:attr:`session` is not available, it raises a :class:`SessionNotAvailable`
exception.'''
        return self.session.add(self)
    
    def delete(self):
        '''Delete the model. If the :attr:`session` is not available,
it raises a :class:`SessionNotAvailable` exception.'''
        return self.session.delete(self)


ModelBase = ModelType('ModelBase', (Model,), {'abstract': True})


def raise_kwargs(model, kwargs):
    if kwargs:
        keys = ', '.join(kwargs)
        if len(kwargs) > 1:
            keys += ' are'
        else:
            keys += ' is an'
        raise ValueError("%s invalid keyword for %s." % (keys, model._meta))

