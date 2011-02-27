from copy import copy

from stdnet.exceptions import *
from stdnet.utils import zip, to_bytestring


class QuerySet(object):
    '''Queryset manager'''
    
    def __init__(self, meta, fargs = None, eargs = None, filter_sets = None):
        '''A query set is  initialized with
        
        * *meta* an model instance meta attribute,
        * *fargs* dictionary containing the lookup parameters to include.
        * *eargs* dictionary containing the lookup parameters to exclude.
        '''
        self._meta  = meta
        self.model  = meta.model
        self.fargs  = fargs
        self.eargs  = eargs
        self.filter_sets = filter_sets
        self.qset   = None
        self._seq   = None
        
    def __repr__(self):
        if self._seq is None:
            s = self.__class__.__name__
            if self.fargs:
                s = '%s.filter(%s)' % (s,self.fargs)
            if self.eargs:
                s = '%s.exclude(%s)' % (s,self.eargs)
            return s
        else:
            return str(self._seq)
    
    def __str__(self):
        return self.__repr__()
    
    def __getitem__(self, slic):
        return self.aslist()[slic]
    
    def filter(self, **kwargs):
        '''Returns a new ``QuerySet`` containing objects that match the given lookup parameters.'''
        if self.fargs:
            c = self.fargs.copy()
            c.update(kwargs)
            kwargs = c
        return self.__class__(self._meta,fargs=kwargs,eargs=self.eargs,filter_sets=self.filter_sets)
    
    def exclude(self, **kwargs):
        '''Returns a new ``QuerySet`` containing objects that do not match the given lookup parameters.'''
        if self.eargs:
            c = self.eargs.copy()
            c.update(kwargs)
            kwargs = c
        return self.__class__(self._meta,fargs=self.fargs,eargs=kwargs,filter_sets=self.filter_sets)
    
    def get(self):
        items = self.aslist()
        if items:
            if len(items) == 1:
                return items[0]
            else:
                raise QuerySetError('Get query yielded non unique results')
        else:
            raise self.model.DoesNotExist
    
    def count(self):
        '''Return the number of objects in ``self`` without
fetching objects.'''
        self.buildquery()
        return len(self.qset)
        
    def __contains__(self, val):
        if isinstance(val,self.model):
            val = val.id
        try:
            val = to_bytestring(val)
        except:
            return False
        self.buildquery()
        return val in self.qset
        
    def __len__(self):
        return self.count()
    
    def buildquery(self):
        '''Build a queryset'''
        if self.qset is not None:
            return
        meta = self._meta
        if self.fargs:
            fargs = self.aggregate(self.fargs)
        else:
            fargs = None
        if self.eargs:
            eargs = self.aggregate(self.eargs)
        else:
            eargs = None
        self.qset = self._meta.cursor.query(meta, fargs, eargs,
                                            filter_sets = self.filter_sets)
        
    def aggregate(self, kwargs):
        '''Aggregate lookup parameters.'''
        unique  = False
        meta    = self._meta
        fields  = meta.dfields
        result  = {}
        # Loop over arguments
        for name,value in kwargs.items():
            names = name.split('__')
            N = len(names)
            # simple lookup for example filter(name = 'pippo')
            if N == 1:
                if name not in fields:
                    raise QuerySetError("Could not filter. Field {0} not defined.".format(name))
                field = fields[name]
                value = (field.serialize(value),)
                unique = field.unique
            # group lookup filter(name_in ['pippo','luca'])
            elif N == 2 and names[1] == 'in':
                name = names[0]
                if name not in fields:
                    raise QuerySetError("Could not filter. Field %s not defined."
                                        .format(name))
                field = fields[name]
                value = tuple((field.serialize(v) for v in value))
                unique = field.unique
            else: 
                # Nested lookup. Not available yet!
                raise NotImplementedError("Nested lookup is not yet available")
            
            result[name] = (value,unique)
            
            if not field.index:
                raise QuerySetError("Field %s is not an index. Cannot query." % name)
        return result
        
    def items(self):
        '''Generator of instances in queryset.'''
        if self._seq is not None:
            for m in self._seq:
                yield m
        else:
            self.buildquery()
            seq = self._seq = []
            meta = self._meta
            for m in meta.cursor.unwind_query(meta,self.qset):
                seq.append(m)
                yield m
    
    def __iter__(self):
        return self.items()
                
    def aslist(self):
        '''Return python ``list`` of elements in queryset'''
        if self._seq is None:
            return list(self.items())
        return self._seq
    
    def delete(self, dlist = None):
        '''Delete all the element in the queryset'''
        T = 0
        for el in self:
            T += el.delete(dlist)
        return T
    

class Manager(object):
    '''A manager class for :class:`stdnet.orm.StdModel` models.'''
    def get(self, **kwargs):
        qs = self.filter(**kwargs)
        return qs.get()
    
    def get_or_create(self, **kwargs):
        '''Get an object. If it does not exists, it creates one'''
        try:
            res = self.get(**kwargs)
            created = False
        except ObjectNotFound:
            res = self.model(**kwargs)
            res.save()
            created = True
        return res,created
    
    def filter(self, **kwargs):
        return QuerySet(self._meta, fargs = kwargs)
    
    def exclude(self, **kwargs):
        return QuerySet(self._meta, eargs = kwargs)

    def all(self):
        '''Return a :class:`QuerySet` which retrieve all instances of the model.'''
        return self.filter()
    
    def _setmodel(self, model):
        meta = model._meta
        self.model    = model
        self._meta    = meta
        self.cursor   = meta.cursor
    
    
class RelatedManager(Manager):
    '''A :class:`Manager` for handling related :class:`stdnet.orm.StdModels`
to a :class:`stdnet.orm.ForeignKey`.'''
    def __init__(self, model, related, fieldname, obj = None):
        self.model      = model
        self.to         = related
        self.fieldname  = fieldname
        self.obj        = obj
    
    def __str__(self):
        return '%s to %s' % (self.model._meta,self.to._meta)
    
    def __get__(self, instance, instance_type=None):
        return self.__class__(self.model,self.to,self.fieldname,instance)
    
    def _get_field(self):
        return self.model._meta.dfields[self.fieldname]
    field = property(_get_field)
    
    def get_related_object(self, id):
        return self.model.objects.get(id = id)
        
    def filter(self, **kwargs):
        if self.obj:
            kwargs[self.fieldname] = self.obj
            return QuerySet(self.to._meta, kwargs)
        else:
            raise QuerySetError('Related manager has no object')
            

class M2MRelatedManager(Manager):
    '''A :class:`RelatedManager` for a :class:`stdnet.orm.ManyToManyField`'''
    def __init__(self, instance, to, st, to_name):
        self.instance = instance
        self.to = to
        self.st = st
        self.to_name = to_name
    
    def add(self, value):
        '''Add *value*, an instance of self.to'''
        if not isinstance(value,self.to):
            raise FieldValueError('%s is not an instance of %s' % (value,self.to._meta))
        if value is self.instance:
            return
        related = getattr(value,self.to_name)
        self._add(value)
        related._add(self.instance)
        
    def _add(self, value):
        self.st.add(value)
        
    def filter(self, **kwargs):
        extrasets = [self.st.id]
        return QuerySet(self.to._meta, kwargs, filter_sets = extrasets)
    

class UnregisteredManager(object):
    
    def __init__(self, model):
        self.model = model
        
    def __getattr__(self, name):
        raise ModelNotRegistered('Model %s is not registered with a backend' % self.model.__name__)


    