import copy
import json

from stdnet.exceptions import *
from stdnet.utils import zip, UnicodeMixin, JSPLITTER, iteritems
from stdnet import dispatch, transaction, attr_local_transaction

from .base import StdNetType, FakeModelType
from .globals import get_model_from_hash
from .signals import *
from .session import Session, Manager


__all__ = ['ModelMixin',
           'FakeModel',
           'StdModel',
           'StdNetType',
           'from_uuid',
           'model_to_dict']


class ModelMixin(UnicodeMixin):
    '''A mixin class for :class:`StdModel`. It implements the :attr:`uuid`
attribute which provides the univarsal unique identifier for an instance of a
model.'''
    DoesNotExist = ObjectNotFound
    '''Exception raised when an instance of a model does not exist.'''
    DoesNotValidate = ObjectNotValidated
    '''Exception raised when an instance of a model does not validate. Usually
raised when trying to save an invalid instance.'''
    
    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return self.id == other.id
        else:
            return False
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        if self.id:
            return hash(self.uuid)
        else:
            return id(self)
        
    @classmethod
    def get_uuid(cls, id):
        return '{0}.{1}'.format(cls._meta.hash,id)
        
    @property
    def uuid(self):
        '''Universally unique identifier for a model instance.'''
        if not self.id:
            raise self.DoesNotExist(\
                    'Object not saved. Cannot obtain universally unique id')
        return self.get_uuid(self.id)
        
    
FakeModelBase = FakeModelType('FakeModelBase',(ModelMixin,),{})
StdNetBase = StdNetType('StdNetBase',(ModelMixin,),{})


class FakeModel(FakeModelBase):
    id = None
    is_base_class = True
    
    
class ModelState(object):
    
    def __init__(self, instance):
        if not instance.is_valid():
            raise FieldValueError(json.dumps(instance.errors))
        self.instance = instance
        self.persistent = False
        self.deleted = False
        dbdata = instance._dbdata
        if instance.id and 'id' in dbdata:
            if instance.id != dbdata['id']:
                raise ValueError('Id has changed from {0} to {1}.'\
                                 .format(instance.id,dbdata['id']))
            self.persistent = True
    
    def __hash__(self):
        return hash(self.instance)
    
    @property    
    def meta(self):
        return self.instance._meta
    
    @property
    def id(self):
        if self.instance.id:
            return self.instance.id
    
    def cleaned_data(self):
        return self.instance._temp['cleaned_data']


class StdModel(StdNetBase):
    '''A :class:`ModelMixin` which gives you the single, definitive source
of data about your data. It contains the essential fields and behaviors
of the data you're storing.

.. attribute:: _meta

    Instance of :class:`Metaclass`, it containes all the information needed
    by a :class:`stdnet.backendServer`.

.. attribute:: id

    Model instance id. The instance primary key.
        
.. attribute:: uuid

    Universally unique identifier for a model instance.
    
.. attribute:: session

    the :class:`Session` instance which loaded the instance (available
    when the instance is loaded from the data server).
'''
    is_base_class = True
    _loadedfields = None
    _state = None
    
    def __init__(self, **kwargs):
        self._dbdata = {}
        for field in self._meta.scalarfields:
            name = field.name
            value = kwargs.pop(name,None)
            if value is None:
                value = field.get_default()
            setattr(self,name,value)
        setattr(self,'id',kwargs.pop('id',None))
        if kwargs:
            raise ValueError("'%s' is an invalid keyword argument for %s" %\
                              (kwargs.keys()[0],self._meta))
            
    def state(self):
        temp = self.temp()
        if 'state' not in temp:
            temp['state'] = ModelState(self)
        return temp['state']

    def loadedfields(self):
        '''Generator of fields loaded from database'''
        if self._loadedfields is None:
            for field in self._meta.scalarfields:
                yield field
        else:
            fields = self._meta.dfields
            processed = set()
            for name in self._loadedfields:
                if name in processed:
                    continue
                if name in fields:
                    processed.add(name)
                    yield fields[name]
                else:
                    name = name.split(JSPLITTER)[0]
                    if name in fields and name not in processed:
                        field = fields[name]
                        if field.type == 'json object':
                            processed.add(name)
                            yield field
    
    def fieldvalue_pairs(self):
        '''Generator of fields,values pairs. Fields correspond to
the ones which have been loaded (usually all of them) or
not loaded but modified.
Check the :ref:`load_only <performance-loadonly>` query function for more
details.'''
        for field in self._meta.scalarfields:
            name = field.attname
            if hasattr(self,name):
                yield field,getattr(self,name)
    
    def save(self, transaction = None, skip_signal = False):
        '''Save the instance.
The model must be registered with a :class:`stdnet.BackendDataServer`
otherwise a :class:`stdnet.ModelNotRegistered` exception will raise.

:parameter transaction: Optional transaction instance.
                        It can be useful when saving
                        several object together (it guarantees atomicity
                        and it is much faster).
                        Check the :ref:`transaction <model-transactions>`
                        documentation for more information.
                        
                        Default: ``None``.
                        
:parameter skip_signal: When saving an instance, :ref:`signals <signal-api>`
                        are sent just before and just after saving. If
                        this flag is set to ``False``, those signals
                        are not used in the function call.
                        
                        Default: ``False``
                        
The method return ``self``.
'''
        if transaction is None:
            cls = self.__class__
            session = cls.objects.session()
            if not skip_signal:
                pre_save.send(sender = cls, instance = self)
            with session.begin():
                session.add(self)
            if not skip_signal:
                post_delete.send(sender=cls, instance = self)
        else:
            transaction.session.add(self)
        return self
    
    def clone(self, id = None, **data):
        '''Utility method for cloning the instance as a new object.
        
:parameter id: Optional new id.
:parameter data: addtitional data to override.
:rtype: an instance of this class
'''
        fields = self.todict()
        fields.update(data)
        fields.pop('id',None)
        fields.pop('__dbdata__',None)
        instance = self._meta.maker()
        instance.__setstate__((id, None, fields))
        return instance
    
    def is_valid(self):
        '''Kick off the validation algorithm by checking all
:attr:`StdModel.loadedfields` against their respective validation algorithm.

:rtype: Boolean indicating if the model validates.'''
        return self._meta.is_valid(self)
    
    def temp(self):
        if not hasattr(self,'_temp'):
            self._temp = {}
        return self._temp
    
    def __get_session(self):
        return self._temp.get('session')
    def __set_session(self,session):
        self._temp['session'] = session
    session = property(__get_session,__set_session)
    
    @property
    def toload(self):
        return self._valattr('toload')
    
    def delete(self, transaction = None):
        '''Delete an instance from database.
If the instance is not available (it does not have an id) and
``StdNetException`` exception will raise.

:parameter transaction: Optional transaction instance as in
                        :meth:`stdnet.orm.StdModel.save`.
'''
        if transaction is None:
            session = self.objects.session()
            with session.transaction():
                session.delete(self)
        else:
            transaction.session.delete(self)
    
    def related_objects(self):
        '''A generator of related objects'''
        objs = []
        for rel in self._meta.related:
            rmanager = getattr(self,rel)
            for obj in rmanager.all():
                yield obj
    
    def todict(self):
        '''Return a dictionary of serialized scalar field for pickling'''
        odict = {}
        for field,value in self.fieldvalue_pairs():
            value = field.serialize(value)
            if value:
                odict[field.name] = value
        if self._dbdata:
            odict['__dbdata__'] = self._dbdata
        return odict
    
    def _to_json(self):
        if self.id:
            yield 'id',self.id
            for field,value in self.fieldvalue_pairs():
                value = field.json_serialize(value)
                if value is not None:
                    yield field.name,value
            
    def tojson(self):
        '''return a JSON serializable dictionary representation.'''
        return dict(self._to_json())
    
    def local_transaction(self, session = None, backend = None, **kwargs):
        '''Create a transaction for this instance.'''
        if not hasattr(self,attr_local_transaction):
            if not session:
                if backend:
                    session = Session(backend)
                else:
                    session = self.objects.session
            setattr(self, attr_local_transaction, session.transaction(**kwargs))
        return getattr(self,attr_local_transaction)
    
    # UTILITY METHODS
    
    @classmethod
    def manager_from_transaction(cls, transaction = None):
        '''Obtain a manager from the transaction'''
        if transaction:
            return Manager(cls, transaction.backend)
        else:
            return cls.objects
    
    # PICKLING SUPPORT
    
    def __getstate__(self):
        return (self.id, self._loadedfields, self.todict())
    
    def __setstate__(self, state):
        id,loadedfields,data = state
        meta = self._meta
        field = meta.pk
        setattr(self,'id',field.to_python(id))
        self._loadedfields = loadedfields
        fields = meta.dfields
        for field in self.loadedfields():
            value = field.value_from_data(self,data)
            setattr(self,field.attname,field.to_python(value))
        self._dbdata = data.get('__dbdata__',{})
    

def from_uuid(uuid):
    '''Retrieve an instance of a :class:`stdnet.orm.StdModel`
from an universal unique identifier *uuid*. If the *uuid* does not match any
instance an exception will raise.'''
    elems = uuid.split('.')
    if len(elems) == 2:
        model = get_model_from_hash(elems[0])
        if not model:
            raise StdModel.DoesNotExist(\
                        'model id "{0}" not available'.format(elems[0]))
        return model.objects.get(id = elems[1])
    raise StdModel.DoesNotExist('uuid "{0}" not recognized'.format(uuid))
    
    
def model_to_dict(instance, fields = None, exclude = None):
    if isinstance(instance,StdModel):
        return instance.todict()
    else:
        d = {}
        for field in instance._meta.fields:
            default = field.get_default()
            if default:
                d[field.name] = default
        return d
                
