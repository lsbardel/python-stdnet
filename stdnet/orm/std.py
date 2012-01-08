from stdnet.exceptions import *
from stdnet.utils import encoders

from .fields import Field
from . import related
from .struct import *


__all__ = ['ManyFieldManagerProxy',
           'Many2ManyManagerProxy',
           'MultiField',
           'SetField',
           'ListField',
           'HashField']


class ManyFieldManagerProxy(object):
    
    def __init__(self, name, cache_name, pickler,
                 value_pickler, scorefun):
        self.name = name
        self.cache_name = cache_name
        self.pickler = pickler
        self.value_pickler = value_pickler
        self.scorefun  = scorefun
    
    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self
        if instance.id is None:
            raise MultiFieldError('id for %s is not available.\
 Call save on instance before accessing %s.' % (instance._meta,self.name))
        cache_name = self.cache_name
        try:
            return getattr(instance, cache_name)
        except AttributeError:
            rel_manager = self.get_related_manager(instance)
            setattr(instance, cache_name, rel_manager)
            return rel_manager
        
    def get_related_manager(self, instance):
        return self.get_structure(instance)
    
    def get_structure(self, instance):
        session = instance.session
        st = getattr(backend,self.stype)
        return st(backend.basekey(instance._meta,'id',instance.id,self.name),
                  instance = instance,
                  #timeout = meta.timeout,
                  pickler = self.pickler,
                  value_pickler = self.value_pickler,
                  scorefun = self.scorefun)


class Many2ManyManagerProxy(ManyFieldManagerProxy):
    
    def __init__(self, name, cache_name, stype, to_name, to):
        super(Many2ManyManagerProxy,self).__init__(name, cache_name, stype,
                                    ModelFieldPickler(to), None, None)
        self.to_name = to_name
        self.model = to
        
    def get_related_manager(self, instance):
        st = self.get_structure(instance)
        return M2MRelatedManager(self.model,
                                 st, self.to_name, instance = instance)


class MultiField(Field):
    '''Virtual class for fields which are proxies to remote
:ref:`data structures <structures-backend>` such as :class:`stdnet.List`,
:class:`stdnet.Set`, :class:`stdnet.OrderedSet` and :class:`stdnet.HashTable`.

Sometimes you want to structure your data model without breaking it up
into multiple entities. For example, you might want to define model
that contains a list of messages an instance receive::

    from stdnet import orm
    
    class MyModel(orm.StdModel):
        ...
        messages = orm.ListField()

By defining structured fields in a model, an instance of that model can access
a stand alone structure in the back-end server with very little effort.


:parameter model: an optional :class:`stdnet.orm.StdModel` class. If
    specified, the structured will contains ids of instances of the model.
    It is saved in the :attr:`relmodel` attribute.
    
.. attribute:: relmodel

    Optional :class:`stdnet.otm.StdModel` class contained in the structure.
    It can also be specified as a string.
    
.. attribute:: pickler

    an instance of :class:`stdnet.utils.encoders.Encoder` used to serialize
    and userialize data. It contains the ``dumps`` and ``loads`` methods.
    
    Default :class:`stdnet.utils.encoders.Json`.
    
.. attribute:: value_pickler

    Same as the :attr:`pickler` attribute, this serializer is applaied to values
    (used by hash table)
    
    Default: ``None``.
'''
    default_pickler = encoders.Json()
    default_value_pickler = None
    
    def __init__(self,
                 model = None,
                 pickler = None,
                 value_pickler = None,
                 required = False,
                 scorefun = None,
                 **kwargs):
        # Force required to be false
        super(MultiField,self).__init__(required = False, **kwargs)
        self.relmodel = model
        self.index = False
        self.unique = False
        self.primary_key = False
        self.pickler = pickler
        self.value_pickler = value_pickler
        self.scorefun = scorefun
        
    def register_with_model(self, name, model):
        super(MultiField,self).register_with_model(name, model)
        if self.relmodel:
            related.load_relmodel(self,self._set_relmodel)
        else:
            self._register_with_model()
    
    def _set_relmodel(self, relmodel):
        self.relmodel = relmodel
        if not self.pickler:
            self.pickler = related.ModelFieldPickler(self.relmodel)
        self._register_with_model()
        
    def _register_with_model(self):
        self._install_encoders()
        self.pickler = self.pickler or self.default_pickler
        self.value_pickler = self.value_pickler or self.default_value_pickler
        setattr(self.model,
                self.name,
                ManyFieldManagerProxy(self.name,
                                      self.get_cache_name(),
                                      pickler = self.pickler,
                                      value_pickler = self.value_pickler,
                                      scorefun = self.scorefun))
    
    def _install_encoders(self):
        if self.relmodel and not self.pickler:
            self.pickler = related.ModelFieldPickler(self.relmodel)

    def add_to_fields(self):
        self.model._meta.multifields.append(self)
        
    def to_python(self, instance):
        return None
    
    def id(self, obj):
        return getattr(obj,self.attname).id

    def todelete(self):
        return True
    
    def structure_class(self):
        raise NotImplementedError


class SetField(MultiField):
    '''A field maintaining an unordered collection of values. It is initiated
without any argument other than an optional model class.
When accessed from the model instance, it returns an instance of
:class:`stdnet.Set` structure. For example::

    class User(orm.StdModel):
        username  = orm.AtomField(unique = True)
        password  = orm.AtomField()
        following = orm.SetField(model = 'self')
    
It can be used in the following way::
    
    >>> user = User(username = 'lsbardel', password = 'mypassword').save()
    >>> user2 = User(username = 'pippo', password = 'pippopassword').save()
    >>> user.following.add(user2)
    >>> user.save()
    >>> user2 in user.following
    True
    '''
    def structure_class(self):
        return Zset if self.ordered else Set
    

class ListField(MultiField):
    '''A field maintaining a list of values.
When accessed from the model instance,
it returns an instance of :class:`stdnet.List` structure. For example::

    class UserMessage(orm.StdModel):
        user = orm.SymbolField()
        messages = orm.ListField()
    
Lets register it with redis::

    >>> orm.register(UserMessage,''redis://127.0.0.1:6379/?db=11')
    'redis db 7 on 127.0.0.1:6379'
    
Can be used as::

    >>> m = UserMessage(user = 'pippo').save()
    >>> m.messages.push_back("adding my first message to the list")
    >>> m.messages.push_back("ciao")
    >>> m.save()
    >>> type(u.messages)
    <class 'stdnet.backends.structures.structredis.List'>
    >>> u.messages.size()
    2
    '''
    type = 'list'
    def structure_class(self):
        return List        


class HashField(MultiField):
    '''A Hash table field, the networked equivalent of a python dictionary.
Keys are string while values are string/numeric.
it returns an instance of :class:`stdnet.HashTable` structure.
'''
    type = 'hash'
    default_pickler = encoders.NoEncoder()
    default_value_pickler = encoders.Json()
    
    def get_pipeline(self):
        return 'hash'
    
    def _install_encoders(self):
        if self.relmodel and not self.value_pickler:
            self.value_pickler = related.ModelFieldPickler(relmodel)

    def structure_class(self):
        return HashTable

