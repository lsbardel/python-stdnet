from stdnet.exceptions import *
from stdnet.utils import encoders

from .fields import Field
from . import related
from .struct import *


__all__ = ['StructureField',
           'StructureFieldProxy',
           'StringField',
           'SetField',
           'ListField',
           'HashField',
           'TimeSeriesField']


class StructureFieldProxy(object):
    '''A descriptor for a :class:`StructureField`.'''
    def __init__(self, field, factory):
        self.field = field
        self.factory = factory
        self.cache_name = field.get_cache_name()
        
    @property
    def name(self):
        return self.field.name
        
    def __get__(self, instance, instance_type=None):
        if not self.field.class_field:
            if instance is None:
                return self
            if instance.id is None:
                raise StructureFieldError('id for %s is not available.\
 Call save on instance before accessing %s.' % (instance._meta,self.name))
        else:
            instance = instance_type
        cache_name = self.cache_name
        cache_val = None
        if self.field.class_field:
            session = instance.objects.session()
        else:
            session = instance.session
        try:
            cache_val = getattr(instance, cache_name)
            if not isinstance(cache_val, Structure):
                raise AttributeError()
            structure = cache_val 
        except AttributeError:
            structure = self.get_structure(instance, session)
            setattr(instance, cache_name, structure)
            if cache_val is not None:
                structure.set_cache(cache_val)
        structure.session = session
        return structure
        
    def get_structure(self, instance, session):
        if self.field.class_field:
            id = session.backend.basekey(instance._meta, 'struct', self.name)
            instance = None
        else:
            id = session.backend.basekey(instance._meta, 'obj', instance.id,
                                         self.name)
        return self.factory(id = id,
                            instance = instance,
                            pickler = self.field.pickler,
                            value_pickler = self.field.value_pickler,
                            **self.field.struct_params)


class StructureField(Field):
    '''Virtual base class for :class:`Field` which are proxies to
:ref:`data structures <model-structures>` such as :class:`List`,
:class:`Set`, :class:`OrderedSet`, :class:`HashTable` and timeseries
:class:`TS`.

Sometimes you want to structure your data model without breaking it up
into multiple entities. For example, you might want to define model
that contains a list of messages an instance receive::

    from stdnet import orm
    
    class MyModel(odm.StdModel):
        ...
        messages = odm.ListField()

By defining structured fields in a model, an instance of that model can access
a stand alone structure in the back-end server with very little effort::

    m = MyModel.objects.get(id=1)
    m.messages.push_back('Hello there!')

Behind the scenes, this functionality is implemented by Python descriptors_.

:parameter model: an optional :class:`StdModel` class. If
    specified, the structured will contains ids of instances of the model and
    it can be accessed via the :attr:`relmodel` attribute.
    It can also be specified as a string if class specification is not possible.
    
**Additional Field attributes**

.. attribute:: relmodel

    Optional :class:`StdModel` class contained in the structure.
    
.. attribute:: value_pickler

    An :class:`stdnet.utils.encoders.Encoder` used to encode and decode
    values.
    
    Default: :class:`stdnet.utils.encoders.Json`.    
    
.. attribute:: pickler

    Same as the :attr:`value_pickler` attribute, this serializer is applied
    to keys, rather than values, in :class:`StructureField`
    of :class:`PairMixin` type (these include :class:`HashField`,
    :class:`TimeSeriesField` and ordered :class:`SetField`)
    
    Default: ``None``.
    
.. attribute:: class_field

    If ``True`` this :class:`StructureField` is a class field (it belongs to
    the model class rather than model instances). For example::
    
        class MyModel(odm.StdModel):
            ...
            updates = odm.List(class_field=True)
            
        MyModel.updates.push_back(1)
    
    Default: ``False``.
    
.. _descriptors: http://users.rcn.com/python/download/Descriptor.htm
'''
    default_pickler = None
    default_value_pickler = encoders.Json()
    
    def __init__(self, model=None, pickler=None, value_pickler=None,
                 class_field=False, **kwargs):
        # Force required to be false
        super(StructureField,self).__init__(**kwargs)
        self.relmodel = model
        self.required = False
        self.index = False
        self.unique = False
        self.primary_key = False
        self.class_field = class_field
        self.pickler = pickler
        self.value_pickler = value_pickler
    
    def _handle_extras(self, **extras):
        self.struct_params = extras
        
    def register_with_model(self, name, model):
        super(StructureField,self).register_with_model(name, model)
        if self.relmodel:
            related.load_relmodel(self,self._set_relmodel)
        else:
            self._register_with_model()
    
    def _set_relmodel(self, relmodel):
        self.relmodel = relmodel
        self._register_with_model()
        
    def _register_with_model(self):
        data_structure_class = self.structure_class()
        self.value_pickler = self.value_pickler or\
                                            data_structure_class.value_pickler
        self.pickler = self.pickler or data_structure_class.pickler or\
                            self.default_pickler
        if not self.value_pickler:
            if self.relmodel:
                self.value_pickler = related.ModelFieldPickler(self.relmodel)
            else:
                self.value_pickler = self.default_value_pickler
        setattr(self.model,
                self.name,
                StructureFieldProxy(self, data_structure_class))

    def add_to_fields(self):
        self.model._meta.multifields.append(self)
        
    def to_python(self, instance):
        return None
    
    def id(self, obj):
        return getattr(obj,self.attname).id

    def todelete(self):
        return True
    
    def structure_class(self):
        raise NotImplementedError()

    def set_cache(self, instance, data):
        setattr(instance,self.get_cache_name(),data)
        

class SetField(StructureField):
    '''A field maintaining an unordered collection of values. It is initiated
without any argument other than an optional model class.
When accessed from the model instance, it returns an instance of
:class:`Set` structure. For example::

    class User(odm.StdModel):
        username  = odm.AtomField(unique = True)
        password  = odm.AtomField()
        following = odm.SetField(model = 'self')
    
It can be used in the following way::
    
    >>> user = User(username='lsbardel', password='mypassword').save()
    >>> user2 = User(username='pippo', password='pippopassword').save()
    >>> user.following.add(user2)
    >>> user.save()
    >>> user2 in user.following
    True
    '''
    def structure_class(self):
        return Zset if self.ordered else Set
    

class ListField(StructureField):
    '''A field maintaining a list of values.
When accessed from the model instance,
it returns a of :class:`List` structure. For example::

    class UserMessage(odm.StdModel):
        user = odm.SymbolField()
        messages = odm.ListField()
    
Lets register it with redis::

    >>> odm.register(UserMessage,''redis://127.0.0.1:6379/?db=11')
    'redis db 7 on 127.0.0.1:6379'
    
Can be used as::

    >>> m = UserMessage(user='pippo').save()
    >>> m.messages.push_back("adding my first message to the list")
    >>> m.messages.push_back("ciao")
    >>> type(u.messages)
    <class 'stdnet.odm.struct.List'>
    >>> u.messages.size()
    2
    '''
    type = 'list'
    def structure_class(self):
        return List        


class HashField(StructureField):
    '''A Hash table field, the networked equivalent of a python dictionary.
Keys are string while values are string/numeric.
it returns an instance of :class:`HashTable` structure.
'''
    type = 'hash'
    default_pickler = encoders.NoEncoder()
    default_value_pickler = encoders.Json()
    
    def _install_encoders(self):
        if self.relmodel and not self.value_pickler:
            self.value_pickler = related.ModelFieldPickler(relmodel)

    def structure_class(self):
        return HashTable


class TimeSeriesField(HashField):
    '''A timeseries field based on :class:`TS` data structure.'''
    type = 'ts'
    default_pickler = None
    
    def structure_class(self):
        return TS
        
        
class StringField(StructureField):
    default_value_pickler = None
    
    def structure_class(self):
        return String
    