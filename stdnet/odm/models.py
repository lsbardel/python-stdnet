import copy
import json

from stdnet.exceptions import *
from stdnet.utils import zip, JSPLITTER, EMPTYJSON, iteritems

from .base import StdNetType, Model
from .session import Session, Manager
from . import signals


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

    def __init__(self, **kwargs):
        kwargs.pop(self._meta.pkname(), None)
        for field in self._meta.scalarfields:
            self.set_field_value(field, kwargs.pop(field.name, None))
        if kwargs:
            keys = ', '.join(kwargs)
            if len(kwargs) > 1:
                keys += ' are'
            else:
                keys += ' is an'
            raise ValueError("%s invalid keyword for %s." % (keys, self._meta))

    @property
    def has_all_data(self):
        '''``True`` if this :class:`StdModel` instance has all back-end data
loaded. This applies to persistent instances only. This property is used when
committing changes. If all data is available, the commit will replace the
previous object data entirely, otherwise it will only update it.'''
        return self.state().persistent and self._loadedfields is None
    
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

    def fieldvalue_pairs(self, exclude_cache=False):
        '''Generator of fields,values pairs. Fields correspond to
the ones which have been loaded (usually all of them) or
not loaded but modified.
Check the :ref:`load_only <performance-loadonly>` query function for more
details.

If *exclude_cache* evaluates to ``True``, fields with :attr:`Field.as_cache`
attribute set to ``True`` won't be included.

:rtype: a generator of two-elements tuples'''
        for field in self._meta.scalarfields:
            if exclude_cache and field.as_cache:
                continue
            name = field.attname
            if hasattr(self, name):
                yield field, getattr(self,name)

    def set_field_value(self, field, value):
        value = field.to_python(value)
        setattr(self, field.attname, value)
        return value

    def clear_cache_fields(self):
        '''Set cache fields to ``None``. Check :attr:`Field.as_cache`
for information regarding fields which are considered cache.'''
        for field in self._meta.scalarfields:
            if field.as_cache:
                setattr(self,field.name,None)

    def get_attr_value(self, attr):
        '''Retrive the *value* for a *attr* name. The attr can be nested,
for example ``group__name``.'''
        if hasattr(self,attr):
            return getattr(self,attr)
        # no atribute, try to check for nested values
        bits = tuple((a for a in attr.split(JSPLITTER) if a))
        if len(bits) > 1:
            instance = self
            for name in bits:
                instance = getattr(instance, name, None)
                if instance is None:
                    return instance
            return instance

    def clone(self, **data):
        '''Utility method for cloning the instance as a new object.

:parameter data: additional which override field data.
:rtype: a new instance of this class.
'''
        meta = self._meta
        pkname = meta.pkname()
        pkvalue = data.pop(pkname, None)
        fields = self.todict(exclude_cache=True)
        fields.update(data)
        fields.pop(pkname,None)
        fields.pop('__dbdata__', None)
        return self._meta.make_object((pkvalue, None, fields))

    def is_valid(self):
        '''Kick off the validation algorithm by checking all
:attr:`StdModel.loadedfields` against their respective validation algorithm.

:rtype: Boolean indicating if the model validates.'''
        return self._meta.is_valid(self)

    def obtain_session(self):
        if self.session is not None:
            return self.session.session()
        else:
            return self.__class__.objects.session()

    def todict(self, exclude_cache=False):
        '''Return a dictionary of serialised scalar field for pickling.
If the *exclude_cache* flag is ``True``, fields with :attr:`Field.as_cache`
attribute set to ``True`` will be excluded.'''
        odict = {}
        for field,value in self.fieldvalue_pairs(exclude_cache=exclude_cache):
            value = field.serialize(value)
            if value:
                odict[field.name] = value
        if 'id' in self._dbdata:
            odict['__dbdata__'] = {'id': self._dbdata['id']}
        return odict

    def _to_json(self, exclude_cache):
        pk = self.pkvalue()
        if pk:
            yield self._meta.pkname(),pk
            for field,value in self.fieldvalue_pairs(exclude_cache=\
                                                     exclude_cache):
                value = field.json_serialize(value)
                if value not in EMPTYJSON:
                    yield field.name,value

    def tojson(self, exclude_cache=True):
        '''return a JSON serializable dictionary representation.'''
        return dict(self._to_json(exclude_cache))

    def load_fields(self, *fields):
        '''Load extra fields to this :class:`StdModel`.'''
        if self._loadedfields is not None:
            meta = self._meta
            kwargs = {meta.pkname(): self.pkvalue()}
            obj = self.__class__.objects.query().load_only(fields).get(**kwargs)
            for name in fields:
                field = meta.dfields.get(name)
                if field is not None:
                    setattr(self,field.attname,getattr(obj,field.attname,None))

    def post_commit(self, callable, **params):
        signals.post_commit.add_callback(lambda *args, **kwargs:\
                                          callable(self, kwargs, **params),
                                          sender=self._meta.model)
        return self
    
    def get_model_attribute(self, name):
        '''Extract an attribute name form this instance.'''
        if JSPLITTER in name and not name.startswith('__'):
            bits = name.split(JSPLITTER)
            fname = bits[0] 
            if fname in self._meta.dfields:
                value = getattr(self, fname, None)
                if value is not None:
                    try:
                        return self._meta.dfields[fname].get_attr(value,
                                                                  bits[1:])
                    except AttributeError:
                        pass
        return getattr(self, name)
    
    def get_state_action(self):        
        if self._loadedfields is None and not\
                getattr(self, '_force_update', False):
            return'override'
        else:
            return 'update'
                
    def load_related_model(self, name, load_only=None, dont_load=None):
        field = self._meta.dfields.get(name)
        if not field:
            raise ValueError('Field %s not available')
        return self._load_related_model(field, load_only, dont_load)
        
    def _load_related_model(self, field, load_only=None, dont_load=None):
        cache_name = field.get_cache_name()
        try:
            return getattr(self, cache_name)
        except AttributeError:
            val = getattr(self, field.attname)
            if val is None:
                return None
            pkname = field.relmodel._meta.pkname()
            qs = self.session.query(field.relmodel)
            if load_only:
                qs = qs.load_only(*load_only)
            if dont_load:
                qs = qs.dont_load(*dont_load)
            try:
                rel_obj = qs.get(**{pkname: val})
            except self.DoesNotExist:
                if field.required:
                    raise
                rel_obj = None
                setattr(self, field.attname, None)
            setattr(self, cache_name, rel_obj)
            return rel_obj
    
    @classmethod
    def from_base64_data(cls, **kwargs):
        o = cls()
        meta = cls._meta
        pkname = meta.pkname()
        for name,value in iteritems(kwargs):
            if name == pkname:
                field = meta.pk
            elif name in meta.dfields:
                field = meta.dfields[name]
            else:
                continue
            value = field.to_python(value)
            setattr(o,field.attname,value)
        return o
    
    # PICKLING SUPPORT

    def __getstate__(self):
        return (self.id, self._loadedfields, self.todict())

    def __setstate__(self, state):
        self._meta.make_object(state)



def model_to_dict(instance, fields=None, exclude=None):
    if isinstance(instance,StdModel):
        return instance.todict()
    else:
        d = {}
        for field in instance._meta.fields:
            default = field.get_default()
            if default:
                d[field.name] = default
        return d

