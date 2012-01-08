from copy import copy
from inspect import isgenerator

from stdnet.exceptions import *
from stdnet.utils import zip, to_bytestring, JSPLITTER

from .signals import *


__all__ = ['Query','QueryOper']


def iterable(value):
    if isgenerator(value) or isinstance(value,(tuple,list,set,frozenset)):
        return True
    else:
        return False
    

class EmptySet(frozenset):
    query_set = ''
    
    def items(self, slic):
        return []
    
    def count(self):
        return len(self)
    
    
class QueryOper(object):
    keyword = ''
    name = ''
    get = 'id'
    
    def __init__(self, qs, underlying):
        self.qs = qs
        self.underlying = underlying
        
    def __repr__(self):
        v = ''
        if self.underlying:
            v = '('+', '.join((str(v) for v in self))+')'
        k = self.keyword
        if self.name:
            k += '-' + self.name
        return k + v
    __str__ = __repr__

    def __iter__(self):
        return iter(self.underlying)
        
    @property
    def meta(self):
        return self.qs._meta
    
    @property
    def backend(self):
        return self.qs.backend
    
    @property
    def ordering(self):
        return self.qs.ordering
    
    @property
    def fields(self):
        return self.qs.fields
    
    @property
    def select_related(self):
        return self.qs._select_related
    
    def flat(self):
        yield self.keyword
        yield self.backend(self.meta)
        yield self.get
        for b in self.body:
            yield b
        yield self.get
        
    
class QuerySet(QueryOper):
    keyword = 'set'
    def __init__(self, qs, name = None, values = None,
                    unique = False, lookup = 'in'):
        values = values if values is not None else ()
        super(QuerySet,self).__init__(qs, values)
        self.name = name or 'id'
        self.unique = unique
        self.lookup = lookup
        
    @property
    def values(self):
        return self.underlying
    
    
class Select(QueryOper):
    """Forms the basis of select type set operations."""
    def __init__(self, qs, keyword, queries):
        super(Select,self).__init__(qs, queries)
        self.keyword = keyword
    
        
def intersect(qs, queries):
    return Select(qs, 'intersect', queries)

def union(qs, queries):
    return Select(qs, 'union', queries)

def difference(qs, queries):
    return Select(qs, 'diff', queries)
    

class Query(object):
    '''A :class:`Query` is produced in terms of a given :class:`Session`,
using the :meth:`Session.query` method::

    qs = session.query(MyModel)
    
A query is equivalent to a collection of SELECT statements for a standard
relational database. It has a  a generative interface whereby successive calls
return a new :class:`Query` object, a copy of the former with additional
criteria and options associated with it.

.. attribute:: _meta

    The :attr:`StdModel._meta` attribute.
    
.. attribute:: session

    The :class:`Session` which created the :class:`Query` via the
    :meth:`Session.query` method.
    
.. attribute:: _get_field

    The :class:`Field` which provides the values of the matched elements.
    This can be changed via the :meth:`get_field` method.
    
    Default: ``id``.
    
.. attribute:: backend

    the :class:`stdnet.BackendDataServer` holding the data to query.
    
.. attribute:: model

    the :class:`StdModel` class for this query.
    
.. attribute:: fargs

    Dictionary containing the ``filter`` lookup parameters each one of
    them corresponding to a ``where`` clause of a select. This value is
    manipulated via the :meth:`filter` method.
    
    Default: ``{}``.
    
.. attribute:: eargs

    Dictionary containing the ``exclude`` lookup parameters each one
    of them corresponding to a ``where`` clause of a select. This value is
    manipulated via the :meth:`exclude` method.
    
    Default: ``{}``.

.. attribute:: ordering

    optional ordering field.
    
.. attribute:: text

    optional text to filter result on.
    
    Default: ``""``.
'''
    start = None
    stop = None
    _get_field = 'id'
    lookups = ('in','contains')
    
    def __init__(self, meta, session, fargs = None, eargs = None,
                 filter_sets = None, ordering = None,
                 text = None, empty = False):
        '''A :class:`Query` is not initialized directly.'''
        self._meta  = meta
        self.session = session
        self.fargs  = fargs
        self.eargs  = eargs
        self.ordering = ordering
        self.filter_sets = filter_sets
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
        '''Return a ``list`` of all matched elements.'''
        return self.items()
    
    def __iter__(self):
        return iter(self.items())
    
    def __len__(self):
        return self.count()
        
    def filter(self, **kwargs):
        '''Create a new :class:`Query` with additional clauses corresponding to
``where`` or ``limit`` in a ``SQL SELECT`` statement.

:parameter kwargs: dictionary of limiting clauses.
:rtype: a new :class:`Query` instance.

For example::

    qs = session.query(MyModel)
    result = qs.filter(group = 'planet')
'''
        if kwargs:
            q = self._clone()
            if self.fargs:
                c = self.fargs.copy()
                c.update(kwargs)
                kwargs = c
            q.fargs = kwargs
            return q
        else:
            return self
    
    def exclude(self, **kwargs):
        '''Returns a new :class:`Query` with additional clauses corresponding
to ``EXCEPT`` in a ``SQL SELECT`` statement.

:parameter kwargs: dictionary of limiting clauses.
:rtype: a new :class:`Query` instance.
'''
        if kwargs:
            q = self._clone()
            if self.eargs:
                c = self.eargs.copy()
                c.update(kwargs)
                kwargs = c
            q.eargs = kwargs
            return q
        else:
            return self
    
    def sort_by(self, ordering):
        '''Sort the query by the given field
        
:parameter ordering: a string indicating the class:`Field` name to sort by.
    If prefixed with ``-``, the sorting will be in descending order, otherwise
    in ascending order.
:return type: a new :class:`Query` instance.
'''
        if ordering:
            ordering = self._meta.get_sorting(ordering,QuerySetError)
        q = self._clone()
        q.ordering = ordering
        return q
    
    def search(self, text):
        '''Search *text* in model. A search engine needs to be installed
for this function to be available.

:parameter text: a string to search.
:return type: a new :class:`Query` instance.
'''
        if self._meta.searchengine:
            q = self._clone()
            q.text = text
            return q
            #return se.search_model(self.model,text)
        else:
            raise QuerySetError('Search not implemented for {0} model'\
                                .format(self.model))
    
    def get_field(self, field):
        '''A :class:`Query` performs a series of operations and utimately
generate of set of matched elements ``ids``. If on the other hand, a
different field is required, it can be specified with the :meth:`get_field`
method. For example, lets say a model has a field called ``object_id``
which contains ids of another model, we could use::

    qs = session.query(MyModel).get_field('object_id')
    
to obtain a set containing the values of matched elements ``object_id``
fields.

:parameter field: the name of the field which will be used to obtained the
    matched elements value. Must be an index.
:rtype: a new :class:`Query` instance.
'''
        if field != self._get_field:
            if field not in self.meta._dfields:
                if field_name not in fields:
                    raise QuerySetError('Model "{0}" has no field "{1}"'\
                                        .format(meta,field_name))
            q = self._clone()
            q._get_field = field
            return q
        else:
            return self
        
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
            
    def get(self, **kwargs):
        '''Return an instance of a model matching the query. A special case is
the query on ``id`` which provides a direct access to the :attr:`session`
instances. If the given primary key is present in the session, the object
is returned directly without performing any query.'''
        id = kwargs.get('id')
        if id is not None and len(kwargs) == 1:
            # check the current session first
            el = self.session.get(self.model, id)
            if el is not None:
                return el
        # not there, perform the database query
        qs = self.filter(**kwargs)
        items = qs.items()
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
This method is efficient since the :class:`Query` does not
receive any data from the server apart from the number of matched elements.
It construct the queries and count the
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
    
    def delete(self, sync_session = False):
        '''Delete all matched elements of the :class:`Query`.'''
        c = self.backend_query().delete(with_ids = sync_session)
        if c:
            post_delete.send(sender=self.__class__, instance = self)
        if sync_session:
            for id in c:
                instance = self.model(id = id)
                self.session.expunge(instance)
        return c
    
    def backend_query(self):
        '''Build and return the :class:`stdnet.BackendQuery`.
This is a lazy method in the sense that it is evaluated once only and its
result stored for future retrieval.'''
        if self.__qset is None:
            self.__qset = self.construct()
        return self.__qset
    
    def test_unique(self, fieldname, value, instance = None, exception = None):
        '''Test if a given field *fieldname* has a unique *value*
in :attr:`model`. The field must be an index of the model.
If the field value is not unique and the *instance* is not the same
an exception is raised.

:parameter fieldname: :class:`Field` name to test
:parameter vale: :class:`Field` value
:parameter instance: optional instance of :attr:`model`
:parameter exception: optional exception class to raise if the test fails.
    Default: :attr:`ModelMixin.DoesNotValidate`.
:return: *value*
'''
        try:
            r = self.get(**{fieldname:value})
        except self.model.DoesNotExist:
            return value
        
        if instance and r.id == instance.id:
            return value
        else:
            exception = exception or self.model.DoesNotValidate
            raise exception('An instance with {0} {1} is already available'\
                            .format(fieldname,value))
    
    ############################################################################
    # PRIVATE METHODS
    ############################################################################
    def _clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        q.__dict__ = self.__dict__.copy()
        q.clear()
        return q
    
    def clear(self):
        self.__qset = None
        self.__slice_cache = None
        
    def cache(self):
        if not self.__slice_cache:
            self.__slice_cache = {}
        return self.__slice_cache
    
    def construct(self):
        if self.__empty:
            return EmptySet()
        q = QuerySet(self)
        if self.fargs:
            args = []
            fargs = self.aggregate(self.fargs)
            for f in fargs:
                # no values to filter on. empty result.
                if not f.values:
                    return EmptySet()
            q = intersect(self, [q]+fargs)
        
        if self.eargs:
            eargs = self.aggregate(self.eargs)
            for a in tuple(eargs):
                if not a.values:
                    eargs.remove(a)
            if eargs:
                if len(eargs) > 1:
                    eargs = [union(self, eargs)]
                q = difference(self, [q]+eargs)
        
        q.get = self._get_field
        return self.backend.Query(self.backend,q)

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
            lookup = '__'.join(names[1:])
            lvalue = field.filter(self.session, lookup, value)
            if lvalue:
                lookup = 'in'
                value = lvalue
            if isinstance(value,self.__class__):
                value = value.backend_query()
            else:
                if not iterable(value):
                    value = (value,) 
                value = tuple((field.index_value(v) for v in value))
                            
            unique = field.unique
            field_name = field.attname
            yield QuerySet(self, field_name, value, unique, lookup)
        
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
            seq = []
            session = self.session
            for el in self.backend_query().items(slic):
                session.server_update(el)
                seq.append(el)
            cache[key] = seq
            return seq
