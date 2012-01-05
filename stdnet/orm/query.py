from copy import copy
from collections import namedtuple

from stdnet.exceptions import *
from stdnet.utils import zip, to_bytestring, JSPLITTER
from stdnet import transaction as get_transaction


__all__ = ['Query']

queryarg = namedtuple('queryarg','meta name values unique lookup')
field_query = namedtuple('field_query','query field')


class EmptySet(frozenset):
    query_set = ''
    
    def items(self, slic):
        return []
    
    def count(self):
        return len(self)
    
    
class QueryOperation(object):
    
    def __init__(self, queries):
        self.queries = queries
        

class Query(object):
    '''A :class:`Query` is produced in terms of a given :class:`Session`,
using the :meth:`Session.query` method::

    qs = session.query(MyModel)
    
.. attribute:: _meta

    the :attr:`StdModel.meta` attribute.
    
.. attribute:: backend

    the :class:`stdnet.BackendDataServer` holding the data to query.
    
.. attribute:: fargs

    Dictionary containing the ``filter`` lookup parameters each one of
    them corresponding to a ``where`` clause of a select.
    
.. attribute:: eargs

    Dictionary containing the ``exclude`` lookup parameters each one
    of them corresponding to a ``where`` clause of a select.

.. attribute:: ordering

    optional ordering field.
    
.. attribute:: text_search

    optional text to filter result on.
'''
    start = None
    stop = None
    lookups = ('in','contains')
    
    def __init__(self, meta, session, fargs = None, eargs = None,
                 filter_sets = None, ordering = None,
                 field_queries = None, text = None,
                 empty = False):
        self._meta  = meta
        self.session = session
        self.fargs  = fargs
        self.eargs  = eargs
        self.ordering = ordering
        self.filter_sets = filter_sets
        self._field_queries = field_queries
        self.text = text
        self._select_related = None
        self.fields = None
        self.__empty = empty
        self.clear()
    
    @property
    def backend(self):
        return self.session.backend
    
    @property
    def model(self):
        return self._meta.model
        
    @property
    def executed(self):
        if self.__qset is not None:
            return self.__qset.executed
        else:
            return False
        
    def clear(self):
        self.simple = False
        self.__qset = None
        self.__slice_cache = None
        
    def cache(self):
        if not self.__slice_cache:
            self.__slice_cache = {}
        return self.__slice_cache
    
    def __repr__(self):
        seq = self.cache().get(None)
        if seq is None:
            s = self.__class__.__name__
            if self.fargs:
                s = '%s.filter(%s)' % (s,self.fargs)
            if self.eargs:
                s = '%s.exclude(%s)' % (s,self.eargs)
            return s
        else:
            return str(seq)
    __str__ = __repr__
    
    def __getitem__(self, slic):
        if isinstance(slic,slice):
            return self.items(slic)
        return self.items()[slic]
    
    def all(self):
        return self
    
    def __iter__(self):
        return iter(self.items())
    
    def _clone(self, fargs, eargs, filter_sets = None):
        return self.__class__(self._meta,
                              self.session,
                              fargs = fargs,
                              eargs = eargs,
                              filter_sets = self.filter_sets,
                              ordering = self.ordering,
                              field_queries = self._field_queries,
                              text = self.text)
        
    def filter(self, filter_sets = None, **kwargs):
        '''Create a new :class:`QuerySet` with limiting clauses corresponding to
where or limit in a SQL select statement.

:parameter filter_sets: optional list of set ids to filter.
    Used by the :class:`stdnet.orm.query.M2MRelatedManager`.
:parameter kwargs: dictionaris of limiting clauses.
:rtype: a :class:`QuerySet` instance.'''
        if kwargs:
            if self.fargs:
                c = self.fargs.copy()
                c.update(kwargs)
                kwargs = c
            return self._clone(kwargs, self.eargs)
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
            return self._clone(self.fargs,kwargs)
        else:
            return self
    
    def sort_by(self, ordering):
        '''Sort the query by the given field'''
        if ordering:
            ordering = self._meta.get_sorting(ordering,QuerySetError)
        self.ordering = ordering
        return self
    
    def search(self, text):
        '''Search *text* in model. A search engine needs to be installed
for this function to be available.'''
        if self._meta.searchengine:
            self.text = text
            return self
            #return se.search_model(self.model,text)
        else:
            raise QuerySetError('Search not implemented for {0} model'\
                                .format(self.model))
    
    def field_queries(self):
        '''return a list of field queries. Field queries
are queries which produce ids for the model and the query is restricted
to these ids.'''
        q = self._field_queries
        if q is None:
            q = []
        if self.text:
            qf = self._meta.searchengine.search_model(self.model,self.text)
            if qf is None:
                self.__empty = True
                return []
            q.extend(qf)
        return q
                
    def load_related(self, *fields):
        '''It returns a new ``QuerySet`` that automatically follows foreign-key
relationships, selecting that additional related-object data when it executes
its query. This is :ref:`performance boost <increase-performance>` when
accessing the related fields of all (most) objects in your queryset.

:parameter fields: fields to include in the loading. If not provided all
    foreign keys and :ref:`structured fields <model-field-structure>`
    in the model will be loaded.
:rtype: an instance of a :class:`stdnet.orm.query.Queryset`.'''
        if not fields:
            fields = []
            for field in self._meta.scalarfields:
                if hasattr(field,'relmodel'):
                    fields.append(field)
            fields.extend(self._meta.multifields)
        else:
            fields = [self._meta.dfields[field] for field in fields]
        self._select_related = fields
        return self
    
    def load_only(self, *fields):
        '''This is provides a :ref:`performance boost <increase-performance>`
in cases when you need to load a subset of fields of your model. The boost
achieved is less than the one obtained when using
:meth:`QuerySet.load_related`, since it does not reduce the number of requests
to the database. However, it can save you lots of bandwidth when excluding
data intensive fields you don't need.
'''
        self.fields = tuple(set(self._load_only(fields))) if fields else None
        return self
    
    def _load_only(self, fields):
        dfields = self._meta.dfields
        for name in fields:
            if name in dfields:
                yield name
            else:
                # It may be a JSONFiled
                na = name.split(JSPLITTER)[0]
                if na in dfields and dfields[na].type == 'json object':
                    yield name
            
    def get(self):
        items = self.items()
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
        return self.backend_query().count()
    
    def querykey(self):
        self.count()
        return self.__qset.query_set
        
    def __contains__(self, val):
        if isinstance(val,self.model):
            val = val.id
        try:
            val = to_bytestring(val)
        except:
            return False
        return self.backend_query().has(val)
        
    def __len__(self):
        return self.count()
    
    def delete(self):
        if not instance.id:
            raise FieldValueError('Cannot delete object. It was never saved.')
        T = 0
        pre_delete.send(sender=self.model, instance = self)
        for obj in instance.related_objects():
            T += self.delete(obj, transaction)
        res = T + self.backend.delete_object(instance, transaction)
        post_delete.send(sender=self.model, instance = self)
        return res
    
    def backend_query(self):
        '''Build and return the backend query. This is a lazy method in the
 sense that it is evaluated once only and its result stored for future
 retrieval. It return an instance of :class:`stdnet.BeckendQuery`
 '''
        if self.__qset is not None:
            return self.__qset
        if self.__empty:
            self.__qset = EmptySet()
        else:
            self.fqueries = self.field_queries()
            if self.__empty:
                self.__qset = EmptySet()
            else:
                if self.fargs:
                    self.simple = not self.filter_sets
                    fargs = self.aggregate(self.fargs)
                    for f in fargs:
                        # no values to filter on. empty result.
                        if not f.values:
                            self.__qset = EmptySet()
                            return self.__qset
                else:
                    fargs = None
                if self.eargs:
                    eargs = self.aggregate(self.eargs)
                    for a in tuple(eargs):
                        if not a.values:
                            eargs.remove(a)
                else:
                    eargs = None
                self.__qset = self.backend.Query(self,
                                                 fargs,
                                                 eargs,
                                                 queries = self.fqueries)
        return self.__qset
    
    # PRIVATE METHODS
    
    def aggregate(self, kwargs):
        return sorted(self._aggregate(kwargs), key = lambda x : x.name)
        
    def _aggregate(self, kwargs):
        '''Aggregate lookup parameters.'''
        meta    = self._meta
        fields  = meta.dfields
        for name,value in kwargs.items():
            names = name.split('__')
            N = len(names)
            field_name = names[0]
            if field_name not in fields:
                raise QuerySetError('Could not filter on model "{0}".\
 Field "{1}" does not exist.'.format(meta,field_name))
            field = fields[field_name]
            if not field.index:
                raise QuerySetError("{0} {1} is not an index.\
 Cannot query.".format(field.__class__.__name__,field_name))
            
            value = value if isinstance(value,self.__class__) else\
                                            (field.index_value(value),)
                                 
            lookup = '__'.join(names[1:])
            lvalue = field.filter(self.session, lookup, value)
            if lvalue:
                lookup = 'in'
                value = lvalue
            value = value if isinstance(value,self.__class__) else\
                                tuple((field.index_value(v) for v in value))            
            unique = field.unique 
            yield queryarg(meta, field_name, value, unique, lookup)
        
    def items(self, slic = None):
        '''Generator of instances in queryset.'''
        cache = self.cache()
        key = None
        seq = cache.get(None)
        if slic:
            if seq:
                seq = seq[slic]
            else:
                key = (slic.start,slic.step,slic.stop)
                
        if seq is not None:
            return seq
        else:
            seq = list(self.backend_query().items(slic))
            cache[key] = seq
            return seq
                
    def aslist(self, slic = None):
        '''Return python ``list`` of elements in queryset'''
        return self.items(slic)
    
    def delete(self, transaction = None):
        '''Delete all the element in the queryset'''
        if self.count():
            commit = False
            if not transaction:
                commit = True
                transaction = self.backend.transaction()
            for el in self:
                el.delete(transaction)
            if commit:
                transaction.commit()
        self.clear()
            
