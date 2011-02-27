from stdnet.exceptions import *
from stdnet import pipelines
from stdnet.orm.related import add_lazy_relation, ModelFieldPickler

from .fields import Field, RelatedObject
from .query import M2MRelatedManager


__all__ = ['ManyFieldManagerProxy',
           'Many2ManyManagerProxy',
           'MultiField',
           'SetField',
           'ListField',
           'HashField',
           #'TSField',
           'ManyToManyField']


class ManyFieldManagerProxy(object):
    
    def __init__(self, name, stype, pickler, converter, scorefun):
        self.name    = name
        self.stype   = stype
        self.pickler = pickler
        self.converter = converter
        self.scorefun  = scorefun
        
    def get_cache_name(self):
        return '_%s_cache' % self.name
    
    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self
        if instance.id is None:
            raise MultiFieldError('id for %s is not available. Call save on instance before accessing %s.' % (instance._meta,self.name))
        cache_name = self.get_cache_name()
        try:
            return getattr(instance, cache_name)
        except AttributeError:
            rel_manager = self.get_related_manager(instance)
            setattr(instance, cache_name, rel_manager)
            return rel_manager
        
    def get_structure(self, instance):
        meta = instance._meta
        pipe = pipelines(self.stype,meta.timeout)
        st = getattr(meta.cursor,pipe.method,None)
        return st(meta.basekey('id',instance.id,self.name),
                  timeout = meta.timeout,
                  pickler = self.pickler,
                  converter = self.converter,
                  scorefun = self.scorefun)
        
    def get_related_manager(self, instance):
        return self.get_structure(instance)


class Many2ManyManagerProxy(ManyFieldManagerProxy):
    
    def __init__(self, name, stype, to_name, to):
        super(Many2ManyManagerProxy,self).__init__(name, stype, ModelFieldPickler(to), None, None)
        self.to_name = to_name
        self.to = to
        
    def get_related_manager(self, instance):
        st = self.get_structure(instance)
        return M2MRelatedManager(instance,self.to,st,self.to_name)


class MultiField(Field):
    '''Virtual class for data-structure fields:
    
.. attribute:: relmodel

    Optional :class:`stdnet.otm.StdModel` class contained in the structure. It can also be specified as a string.
    
.. attribute:: related_name

    Same as :class:`stdnet.orm.ForeignKey` Field.
    
.. attribute:: pickler

    a module/class/objects used to serialize values. Default ``None``.
    
.. attribute:: converter

    a module/class/objects used to convert keys to suitable string to use as keys in :class:`stdnet.HashTable` structures.
    It must implement two methods, ``tokey`` to convert key to a suitable key
    for the database backend and ``tovalue`` the inverse operation. Default: ``None``.'''
    def get_pipeline(self):
        raise NotImplementedError
    
    def __init__(self,
                 model = None,
                 pickler = None,
                 converter = None,
                 required = False,
                 related_name = None,
                 scorefun = None,
                 **kwargs):
        # Force required to be false
        super(MultiField,self).__init__(required = False,
                                        **kwargs)
        self.relmodel     = model
        self.index        = False
        self.unique       = False
        self.primary_key  = False
        self.related_name = related_name  
        self.pickler      = pickler
        self.converter    = converter
        self.scorefun     = scorefun
        
    def register_with_model(self, name, model):
        super(MultiField,self).register_with_model(name, model)
        if self.relmodel:
            add_lazy_relation(self,self.relmodel,self._register_related_model)
        else:
            self._register_related_model(self,None)
            
    def _register_related_model(self, field, related):
        field.relmodel = related
        if related:
            if not field.pickler:
                field.pickler = ModelFieldPickler(related)
        setattr(self.model,
                self.name,
                ManyFieldManagerProxy(self.name,self.get_pipeline(),self.pickler,self.converter,self.scorefun))

    def add_to_fields(self):
        self.model._meta.multifields.append(self)
        
    def to_python(self, instance):
        return None
    
    def id(self, obj):
        return getattr(obj,self.attname).id


class SetField(MultiField):
    '''A field maintaining an unordered collection of values. It is initiated
without any argument other than an optional model class.
When accessed from the model instance, it returns an instance of :class:`stdnet.Set` structure.
For example::

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
    type = 'set'
    def get_pipeline(self):
        return 'oset' if self.ordered else 'set'
    

class ListField(MultiField):
    '''A field maintaining a list of values. When accessed from the model instance,
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
    def get_pipeline(self):
        return 'list'          


class HashField(MultiField):
    '''A Hash table field, the networked equivalent of a python dictionary.
Keys are string while values are string/numeric.
it returns an instance of :class:`stdnet.HashTable` structure.
'''
    type = 'hash'
    def get_pipeline(self):
        return 'hash'


class ManyToManyField(MultiField):
    '''A many-to-many relationship. It accepts **related_name** as extra argument.

.. attribute:: related_name

    Optional name to use for the relation from the related object
    back to ``self``.
    
    
For example::
    
    class User(orm.StdModel):
        name      = orm.SymbolField(unique = True)
        following = orm.ManyToManyField(model = 'self',
                                        related_name = 'followers')
    
To use it::

    >>> u = User(name = 'luca').save()
    >>> u.following.add(User(name = 'john').save())
    >>> u.following.add(User(name = 'mark').save())
    
    
This field is implemented as a double Set field.
'''
    type = 'many-to-many'
    def get_pipeline(self):
        return 'set'
    
    def register_with_model(self, name, model):
        Field.register_with_model(self, name, model)
        add_lazy_relation(self,self.relmodel,self._register_related_model)
    
    def _register_related_model(self, field, related):
        #Register manager to self and to the related model
        related_name = self.related_name or '%s_set' % self.name
        self.related_name = related_name
        field.relmodel = related
        stype = self.get_pipeline()
        setattr(self.model,  self.name,    Many2ManyManagerProxy(self.name,    stype, related_name, related))
        setattr(self.relmodel,related_name,Many2ManyManagerProxy(related_name, stype, self.name, self.model))
           
