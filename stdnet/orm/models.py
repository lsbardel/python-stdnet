import copy

from stdnet.exceptions import *
from stdnet.utils import zip, UnicodeMixin, JSPLITTER
from stdnet import dispatch, transaction, attr_local_transaction

from .base import StdNetType, FakeModelType
from .globals import get_model_from_hash
from .signals import *


__all__ = ['FakeModel',
           'StdModel',
           'StdNetType',
           'from_uuid',
           'model_to_dict']


class ModelMixin(UnicodeMixin):
    DoesNotExist = ObjectNotFound
    DoesNotValidate = ObjectNotValidated
    
    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return self.id == other.id
        else:
            return False
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        try:
            return hash(self.uuid)
        except self.DoesNotExist as e:
            raise TypeError(str(e))
        
    @property
    def uuid(self):
        '''Universally unique identifier for a model instance.'''
        if not self.id:
            raise self.DoesNotExist(\
                    'Object not saved. Cannot obtain universally unique id')
        return '{0}.{1}'.format(self._meta.hash,self.id)
        
    
FakeModelBase = FakeModelType('FakeModelBase',(ModelMixin,),{})
StdNetBase = StdNetType('StdNetBase',(ModelMixin,),{})


class FakeModel(FakeModelBase):
    id = None
    is_base_class = True


class StdModel(StdNetBase):
    '''A model is the single, definitive source of data
about your data. It contains the essential fields and behaviors
of the data you're storing. Each model class
maps instances to :class:`stdnet.HashTable` structures via
the :attr:`StdModel._meta` attribute.

.. attribute:: _meta

    Instance of :class:`stdnet.orm.base.Metaclass`

.. attribute:: id

    Model instance id. The instance primary key.
        
.. attribute:: uuid

    Universally unique identifier for a model instance.
    
'''
    is_base_class = True
    _loadedfields = None
    
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
The model must be registered with a :class:`stdnet.backends.BackendDataServer`
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
        send_signal = not transaction and not skip_signal
        if send_signal:
            pre_save.send(sender=self.__class__, instance = self)
        meta = self._meta
        if not meta.cursor:
            raise ModelNotRegistered("Model '{0}' is not registered with a\
 backend database. Cannot save instance.".format(meta))
        r = meta.cursor.save_object(self, transaction)
        if send_signal:
            post_save.send(sender=self.__class__,
                           instance = r)
        return r
    
    def save_as_new(self, id = None, commit = True, **kwargs):
        '''Utility method for saving the instance as a new object.
        
:parameter id: Optional new id.
:parameter commit: If ``True`` the :meth:`save` method will be called with
    parameters *kwargs*, otherwise no save performed.
:rtype: an instance of this class
'''
        self.on_save_as_new()
        self._loadedfields  = None
        self.id = id
        self._dbdata = {}
        if commit:
            return self.save(**kwargs)
        else:
            return self
    
    def is_valid(self):
        '''Kick off the validation algorithm by checking oll
:attr:`StdModel.loadedfields` agains their respective validation algorithm.

:rtype: Boolean indicating if the model validates.'''
        return self._meta.is_valid(self)

    def _valattr(self, name):
        if not hasattr(self,'_validation'):
            self.is_valid()
        return getattr(self,'_validation')[name]
            
    @property
    def cleaned_data(self):
        return self._valattr('data')
        
    @property
    def errors(self):
        return self._valattr('errors')
        
    @property
    def indices(self):
        return self._valattr('indices')
    
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
        meta = self._meta
        if not self.id:
            raise StdNetException('Cannot delete object. It was never saved.')
        T = 0
        # Gather related objects to delete
        pre_delete.send(sender=self.__class__, instance = self)
        for obj in self.related_objects():
            T += obj.delete(transaction)
        res = T + meta.cursor.delete_object(self, transaction)
        post_delete.send(sender=self.__class__, instance = self)
        return res
    
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
    
    def local_transaction(self, **kwargs):
        '''Create a transaction for this instance.'''
        if not hasattr(self,attr_local_transaction):
            setattr(self,attr_local_transaction,
                    self._meta.cursor.transaction(**kwargs))
        return getattr(self,attr_local_transaction)
    
    # HOOKS
    
    def on_save_as_new(self):
        '''Callback by :meth:`save_as_new` just before saving an instance as a
new object in the database'''
        pass
    
    # UTILITY METHODS
    
    def instance_keys(self):
        '''Utility method for returning keys associated with
this instance only. The instance id
is however available in other keys (indices and other backend containers).'''
        return self._meta.cursor.instance_keys(self)
    
    @classmethod
    def flush(cls):
        '''Flush the model table and all related tables including all indexes.
Calling flush will erase everything about the model
instances in the remote server. If count is a dictionary, the method
will enumerate the number of object to delete. without deleting them.'''
        return cls._meta.flush()
    
    @classmethod
    def transaction(cls, *models, **kwargs):
        '''Return a transaction instance.'''
        return transaction(cls,*models,**kwargs)
    
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
                
