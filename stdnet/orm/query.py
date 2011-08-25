from copy import copy
from collections import namedtuple

from stdnet.exceptions import *
from stdnet import pipelines
from stdnet.utils import zip, to_bytestring


queryarg = namedtuple('queryarg','name values unique lookup')
field_query = namedtuple('field_query','query field')

class EmptySet(frozenset):
    query_set = ''
    
    def items(self, slic):
        raise StopIteration
    
    def count(self):
        return len(self)
    

class QuerySet(object):
    '''A QuerySet is not created on its but instead using the model manager.
For example::

    qs = MyModel.objects.filter(field = 'bla)
    
``qs`` is a queryset instance for model ``MyModel``.
'''
    start = None
    stop = None
    lookups = ('in','contains')
    
    def __init__(self, meta, fargs = None, eargs = None,
                 filter_sets = None, ordering = None,
                 queries = None, empty = False):
        '''A query set is  initialized with
        
        * *meta* an model instance meta attribute,
        * *fargs* dictionary containing the lookup parameters to include.
        * *eargs* dictionary containing the lookup parameters to exclude.
        '''
        self._meta  = meta
        self.model  = meta.model            
        self.fargs  = fargs
        self.eargs  = eargs
        self.ordering = ordering
        self.filter_sets = filter_sets
        self.queries = queries
        self.clear()
        if empty:
            self.qset = EmptySet()
        
    def clear(self):
        self.simple = False
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
        if isinstance(slic,slice):
            return self.aslist(slic)
        return self.aslist()[slic]
    
    def all(self):
        return self
    
    def __iter__(self):
        return self.items()
        
    def filter(self, **kwargs):
        '''Returns a new ``QuerySet`` containing objects that match the\
 given lookup parameters.'''
        if kwargs:
            if self.fargs:
                c = self.fargs.copy()
                c.update(kwargs)
                kwargs = c
            return self.__class__(self._meta,
                                  fargs=kwargs,
                                  eargs=self.eargs,
                                  filter_sets=self.filter_sets,
                                  ordering=self.ordering,
                                  queries=self.queries)
        else:
            return self
    
    def exclude(self, **kwargs):
        '''Returns a new ``QuerySet`` containing objects that do not match\
 the given lookup parameters.'''
        if kwargs:
            if self.eargs:
                c = self.eargs.copy()
                c.update(kwargs)
                kwargs = c
            return self.__class__(self._meta,
                                  fargs=self.fargs,
                                  eargs=kwargs,
                                  filter_sets=self.filter_sets,
                                  ordering=self.ordering,
                                  queries=self.queries)
        else:
            return self
    
    def sort_by(self, ordering):
        '''Sort the query by the given field'''
        if ordering:
            ordering = self._meta.get_sorting(ordering,QuerySetError)
        self.ordering = ordering
        return self
    
    def search(self, text):
        '''Search text in model. A search engine needs to be installed
for this function to be available.'''
        se = self._meta.searchengine
        if se:
            return se.search_model(self.model,text)
        else:
            raise QuerySetError('Search not implemented for {0} model'\
                                .format(self.model))
    
    def get(self):
        items = self.aslist()
        if items:
            if len(items) == 1:
                return items[0]
            else:
                raise QuerySetError('Get query {0} yielded non\
 unique results'.format(self))
        else:
            raise self.model.DoesNotExist
    
    def count(self):
        '''Return the number of objects in ``self``.
This method is efficient since the queryset does not
receive any data from the server. It construct the queries and count the
objects on the server side.'''
        self._buildquery()
        return self.qset.count()
    
    def querykey(self):
        self.count()
        return self.qset.query_set
        
    def __contains__(self, val):
        if isinstance(val,self.model):
            val = val.id
        try:
            val = to_bytestring(val)
        except:
            return False
        self._buildquery()
        return self.qset.has(val)
        
    def __len__(self):
        return self.count()
    
    # PRIVATE METHODS
    
    def _buildquery(self):
        # Build a queryset from filters and exclude arguments
        if self.qset is not None:
            return 
        meta = self._meta
        if self.fargs:
            self.simple = not self.filter_sets
            fargs = self.aggregate(self.fargs)
        else:
            fargs = None
        if self.eargs:
            self.simple = False
            eargs = self.aggregate(self.eargs)
        else:
            eargs = None
        if self.queries:
            self.simple = False
        self.qset = self._meta.cursor.Query(self,fargs,eargs,
                                            queries = self.queries)
    
    def aggregate(self, kwargs):
        return sorted(self._aggregate(kwargs), key = lambda x : x.name)
        
    def _aggregate(self, kwargs):
        '''Aggregate lookup parameters.'''
        meta    = self._meta
        fields  = meta.dfields
        for name,value in kwargs.items():
            names = name.split('__')
            N = len(names)
            lookup = 'in'
            # simple lookup for example filter(name = 'pippo')
            if N == 1:
                if name not in fields:
                    raise QuerySetError('Could not filter on model "{0}".\
 Field "{1}" does not exist.'.format(meta,name))
                field = fields[name]
                value = (field.serialize(value),)
                unique = field.unique
            # group lookup filter(name_in ['pippo','luca'])
            elif N == 2 and names[1] in self.lookups:
                name = names[0]
                if name not in fields:
                    raise QuerySetError("Could not filter.\
 Field %s not defined.".format(name))
                field = fields[name]
                value = tuple((field.serialize(v) for v in value))
                unique = field.unique
                lookup = names[1]
            else: 
                # Nested lookup. Not available yet!
                raise NotImplementedError("Nested lookup is not yet available")
            
            if not field.index:
                raise QuerySetError("Field %s is not an index.\
 Cannot query." % name)
            elif value:
                self.simple = self.simple and unique 
                yield queryarg(name,value,unique,lookup)
        
    def items(self, slic = None):
        '''Generator of instances in queryset.'''
        if self._seq is not None:
            for m in self._seq:
                yield m
        else:
            self._buildquery()
            seq = self._seq = []
            for m in self.qset.items(slic):
                seq.append(m)
                yield m
                
    def aslist(self, slic = None):
        '''Return python ``list`` of elements in queryset'''
        if self._seq is None:
            return list(self.items(slic))
        return self._seq
    
    def delete(self, transaction = None):
        '''Delete all the element in the queryset'''
        if self.count():
            commit = False
            if not transaction:
                commit = True
                transaction = self._meta.cursor.transaction()
            for el in self:
                el.delete(transaction)
            if commit:
                transaction.commit()
        self.clear()
            

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
    
    def from_queries(self, queries):
        return QuerySet(self._meta, queries = queries)
    
    def exclude(self, **kwargs):
        return QuerySet(self._meta, eargs = kwargs)
    
    def search(self, text):
        return QuerySet(self._meta).search(text)
    
    def empty(self):
        return QuerySet(self._meta, empty = True)
    
    def all(self):
        '''Return a :class:`QuerySet` which retrieve all instances\
 of the model.'''
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
            

class GetStructureMixin(object):
    
    def get_structure(self, instance):
        meta = instance._meta
        transaction = getattr(instance,'_transaction',None)
        pipe = pipelines(self.stype,meta.timeout)
        st = getattr(meta.cursor,pipe.method)
        return st(meta.basekey('id',instance.id,self.name),
                  timeout = meta.timeout,
                  pickler = self.pickler,
                  converter = self.converter,
                  scorefun = self.scorefun,
                  cachepipes = instance._cachepipes,
                  transaction = transaction)
        

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
            raise FieldValueError(
                        '%s is not an instance of %s' % (value,self.to._meta))
        if value not in self.st:
            related = getattr(value,self.to_name)
            self._add(value)
            related._add(self.instance)
            
    def remove(self, value):
        if not isinstance(value,self.to):
            raise FieldValueError(
                        '%s is not an instance of %s' % (value,self.to._meta))
        if value in self.st:
            related = getattr(value,self.to_name)
            self.st.discard(value)
            related.st.discard(self.instance)
        
    def _add(self, value):
        self.st.add(value)
        self.instance.save()
        
    def filter(self, **kwargs):
        extrasets = [self.st.id]
        return QuerySet(self.to._meta, kwargs, filter_sets = extrasets)
    

class UnregisteredManager(object):
    
    def __init__(self, model):
        self.model = model
        
    def __getattr__(self, name):
        raise ModelNotRegistered('Model %s is not registered with\
 a backend' % self.model.__name__)


    