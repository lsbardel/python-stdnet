'''Defines Metaclasses and Base classes for stdnet Models.'''
import sys
from copy import copy, deepcopy
import hashlib
import weakref

from stdnet import BackendRequest, AsyncObject
from stdnet.utils import zip, to_string
from stdnet.exceptions import *

from . import signals
from .globals import hashmodel, JSPLITTER, get_model_from_hash
from .fields import Field, AutoField, orderinginfo
from .session import Manager, setup_managers


__all__ = ['Metaclass',
           'Model',
           'ModelBase',
           'autoincrement',
           'ModelType', # Metaclass for all stdnet ModelBase classes
           'StdNetType', # derived from ModelType, metaclass fro StdModel
           'from_uuid']


def get_fields(bases, attrs):
    fields = {}
    for base in bases:
        if hasattr(base, '_meta'):
            fields.update(deepcopy(base._meta.dfields))

    for name,field in list(attrs.items()):
        if isinstance(field,Field):
            fields[name] = attrs.pop(name)

    return fields


def make_app_label(new_class, app_label = None):
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
    '''A class for storing meta data of a :class:`Model` class.'''
    def __init__(self, model, app_label=None, modelkey=None, abstract=False):
        self.abstract = abstract
        self.model = model
        self.model._meta = self
        self.app_label = make_app_label(model, app_label)
        self.name = model.__name__.lower()
        if not modelkey:
            if self.app_label:
                modelkey = '{0}.{1}'.format(self.app_label,self.name)
            else:
                modelkey = self.name
        self.modelkey = modelkey
        if not abstract:
            hashmodel(model)

    def pkname(self):
        return 'id'

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
which maps a :class:`StdModel` into an object in the in a remote
:class:`stdnet.BackendDataServer`.
An instance is initiated by the odm when a :class:`StdModel` class is created.

To override default behaviour you can specify the ``Meta`` class as an inner
class of :class:`StdModel` in the following way::

    from datetime import datetime
    from stdnet import odm

    class MyModel(odm.StdModel):
        timestamp = odm.DateTimeField(default = datetime.now)
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
    This attribute is set by the ``odm`` during class initialization.

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

    def __init__(self, model, fields, abstract=False, app_label='',
                 verbose_name=None, ordering=None, modelkey=None, **kwargs):
        super(Metaclass,self).__init__(model,
                                       app_label=app_label,
                                       modelkey=modelkey,
                                       abstract=abstract)
        self.fields = []
        self.scalarfields = []
        self.indices = []
        self.multifields = []
        self.dfields = {}
        self.timeout = 0
        self.related = {}
        self.verbose_name = verbose_name or self.name
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
            pk = AutoField(primary_key=True)
        pk.register_with_model(pkname, model)
        fields.pop(pkname, None)
        self.pk = pk
        for name,field in fields.items():
            field.register_with_model(name, model)
        self.ordering = None
        if ordering:
            self.ordering = self.get_sorting(ordering, ImproperlyConfigured)

    def pkname(self):
        return self.pk.name

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
                    if field.type == 'json object':
                        processed.add(name)
                        names.append(name)
                        atts.append(name)
        return names,atts

    def as_dict(self):
        '''Model metadata in a dictionary'''
        pk = self.pk
        id_type = 3
        id_fields = None
        if pk.type == 'auto':
            id_type = 1
        elif pk.type == 'composite':
            id_type = 2
            id_fields = pk.fields
        return {'id_name': pk.name,
                'id_type': id_type,
                'id_fields': id_fields,
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
    '''StdModel python metaclass'''
    is_base_class = True
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, ModelType)]
        if not parents or attrs.pop('is_base_class', False):
            return super(ModelType, cls).__new__(cls, name, bases, attrs)
        return cls.make(name, bases, attrs, attrs.pop('Meta', None))

    @classmethod
    def make(cls, name, bases, attrs, meta):
        new_class = type.__new__(cls, name, bases, attrs)
        ModelMeta(new_class)
        return new_class


class StdNetType(ModelType):
    is_base_class = True
    @classmethod
    def make(cls, name, bases, attrs, meta):
        if meta:
            kwargs = meta_options(**meta.__dict__)
        else:
            kwargs = meta_options()

        # remove and build field list
        fields = get_fields(bases, attrs)
        # create the new class
        new_class = type.__new__(cls, name, bases, attrs)
        setup_managers(new_class)
        app_label = kwargs.pop('app_label')
        Metaclass(new_class, fields, app_label=app_label, **kwargs)
        signals.class_prepared.send(sender=new_class)
        return new_class


def meta_options(abstract=False,
                 app_label=None,
                 ordering=None,
                 modelkey=None,
                 unique_together=None,
                 **kwargs):
    return {'abstract': abstract,
            'app_label':app_label,
            'ordering':ordering,
            'modelkey':modelkey,
            'unique_together':unique_together}


class ModelState(object):
    __slots__ = ('_persistent','deleted','_iid','score')
    def __init__(self, instance):
        self._persistent = False
        self.deleted = False
        self.score = 0
        dbdata = instance._dbdata
        pkname = instance._meta.pkname()
        pkvalue = getattr(instance, pkname)
        if pkvalue and pkname in dbdata:
            if pkvalue != dbdata[pkname]:
                raise ValueError('Id has changed from {0} to {1}.'\
                                 .format(pkvalue,dbdata[pkname]))
            self._persistent = True
        elif not pkvalue:
            pkvalue = 'new.{0}'.format(id(instance))
        self._iid = pkvalue

    @property
    def persistent(self):
        return self._persistent

    @property
    def iid(self):
        return self._iid

    @property
    def action(self):
        return 'delete' if self.deleted else 'save'

    def __repr__(self):
        return self.iid + (' deleted' if self.deleted else '')
    __str__ = __repr__


class Model(AsyncObject):
    '''This is the base class for both :class:`StdModel` and :class:`Structure`
classes. It implements the :attr:`uuid` attribute which provides the universal
unique identifier for an instance of a model.'''
    _model_type = None
    objects = None
    DoesNotExist = ObjectNotFound
    '''Exception raised when an instance of a model does not exist.'''
    DoesNotValidate = ObjectNotValidated
    '''Exception raised when an instance of a model does not validate. Usually
raised when trying to save an invalid instance.'''

    def __new__(cls, *args, **kwargs):
        o = super(Model,cls).__new__(cls)
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
        return hash(self.get_uuid(self.state().iid))

    def state(self, update=False):
        if 'state' not in self._dbdata or update:
            self._dbdata['state'] = ModelState(self)
        return self._dbdata['state']

    def pkvalue(self):
        return getattr(self,self._meta.pkname())

    @classmethod
    def get_uuid(cls, id):
        return '{0}.{1}'.format(cls._meta.hash,id)

    @property
    def uuid(self):
        '''Universally unique identifier for an instance of a :class:`Model`.'''
        if not self.id:
            raise self.DoesNotExist(\
                    'Object not saved. Cannot obtain universally unique id')
        return self.get_uuid(self.id)

    def __get_session(self):
        return self._dbdata.get('session')
    def __set_session(self,session):
        self._dbdata['session'] = session
    session = property(__get_session,__set_session)

    def get_session(self, use_current_session = True):
        if use_current_session:
            session = self.session
            if session is None:
                session = self.obtain_session()
        else:
            session = self.obtain_session()
        if session is None:
            raise SessionNotAvailable('No session available')
        return session

    def obtain_session(self):
        pass

    def save(self, use_current_session=True):
        '''A direct method for saving an object. This method is provided for
convenience and should not be used when using a :class:`Transaction`.
This method always commit changes immediately and if the :class:`Session`
has already started a :class:`Transaction` an error will occur.
If a session is not available, it tries to create one
from its :class:`Manager`.'''
        with self.get_session(use_current_session).begin() as t:
            t.add(self)
        if t.pending:
            return t.pending.add_callback(lambda r: self)
        else:
            return self

    def delete(self, use_current_session = True):
        session = self.get_session(use_current_session)
        with session.begin():
            session.delete(self)
        return self

    def force_save(self):
        '''Same as the :meth:`save` method, however this method never
uses the current session. Instead it creates a new one for immediate commit.'''
        return self.save(use_current_session = False)

    def force_delete(self):
        '''Same as the :meth:`delete` method, however this method never
uses the current session. Instead it creates a new one for immediate commit.'''
        return self.delete(use_current_session = False)


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
