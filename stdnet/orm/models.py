import copy
import json

from stdnet.exceptions import *
from stdnet.utils import zip, JSPLITTER, iteritems

from .base import StdNetType, Model
from .signals import *
from .session import Session, Manager


__all__ = ['StdModel', 'model_to_dict']
        
    
StdNetBase = StdNetType('StdNetBase',(Model,),{})


class StdModel(StdNetBase):
    '''A :class:`Model` which contains data in :class:`Field`.

.. attribute:: _meta

    Instance of :class:`Metaclass`, it containes all the information needed
    by a :class:`stdnet.backendServer`.

.. attribute:: id

    The instance primary key.
        
.. attribute:: uuid

    Universally unique identifier for an instance.
    
.. attribute:: session

    the :class:`Session` instance which loaded the instance (available
    when the instance is loaded from the data server).
'''
    _model_type = 'object'
    is_base_class = True
    _loadedfields = None
    _state = None
    
    def __init__(self, **kwargs):
        for field in self._meta.scalarfields:
            name = field.name
            value = kwargs.pop(name,None)
            if value is None:
                value = field.get_default()
            setattr(self,name,value)
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
        '''Return a dictionary of serialised scalar field for pickling'''
        odict = {}
        for field,value in self.fieldvalue_pairs():
            value = field.serialize(value)
            if value:
                odict[field.name] = value
        if 'id' in self._dbdata:
            odict['__dbdata__'] = {'id':self._dbdata['id']}
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
                
