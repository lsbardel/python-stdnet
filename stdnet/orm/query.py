from copy import copy
from inspect import isgenerator

from stdnet.exceptions import *
from stdnet.utils import zip, to_bytestring, JSPLITTER

from .signals import *


__all__ = ['Query','QueryElement']


def iterable(value):
    if isgenerator(value) or isinstance(value,(tuple,list,set,frozenset)):
        return True
    else:
        return False
    
    
class Q(object):
    keyword = ''
    name = ''
    def __init__(self, meta = None, session = None, select_related = None,
                 ordering = None, fields = None, get_field = None,
                 name = None, keyword = None):
        self.data = {'meta':meta,
                     'session':session,
                     'select_related': select_related,
                     'ordering': ordering,
                     'fields': fields,
                     'get_field': get_field}
        self.name = name if name is not None else self.name
        self.keyword = keyword if keyword is not None else self.keyword
    
    @property
    def _meta(self):
        return self.data['meta']
    
    @property
    def meta(self):
        return self.data['meta']
    
    @property
    def model(self):
        return self._meta.model
    
    @property
    def session(self):
        return self.data['session']
    
    @property
    def select_related(self):
        return self.data['select_related']
    
    @property
    def ordering(self):
        return self.data['ordering']
    
    @property
    def fields(self):
        return self.data['fields']
    
    @property
    def _get_field(self):
        return self.data['get_field']
        
    @property
    def backend(self):
        return self.session.backend
    
    def construct(self):
        raise NotImplementedError()
    
    
class EmptyQuery(Q):
    keyword = 'empty'
    def items(self, slic):
        return []
    
    def __len__(self):
        return 0
    
    def count(self):
        return 0
    
    def construct(self):
        return None
    
    
class QueryElement(Q):
    '''An element of a :class:`Query`.
    
.. attribute:: qs

    the :class:`Query` which contains this :class:`QueryElement`.
    
.. attribute:: underlying

    the element contained in the :class:`QueryElement`. This underlying is
    an iterable or another :class:`Query`.
    
.. attribute:: valid

    if ``False`` this :class:`QueryElement` has no underlying elements and
    won't produce any query.
'''
    def __init__(self, *args, **kwargs):
        self.__backend_query = None
        underlying = kwargs.pop('underlying',None)
        super(QueryElement,self).__init__(*args,**kwargs)
        self.underlying = underlying if underlying is not None else ()
    
    def __repr__(self):
        v = ''
        if self.underlying is not None:
            v = '('+', '.join((str(v) for v in self))+')'
        k = self.keyword
        if self.name:
            k += '-' + self.name
        return k + v
    __str__ = __repr__

    def __iter__(self):
        return iter(self.underlying)
    
    def __len__(self):
        return len(self.underlying)
    
    def construct(self):
        return self
    
    def backend_query(self, **kwargs):
        if self.__backend_query is None:
            self.__backend_query = self.backend.Query(self, **kwargs)
        return self.__backend_query
    
    @property
    def valid(self):
        return bool(self.underlying)
    
    def flat(self):
        yield self.keyword
        yield self.backend(self.meta)
        yield self.get
        for b in self.body:
            yield b
        yield self.get
        
    
class QuerySet(QueryElement):
    '''A :class:`QueryElement` which represents a lookup on a field.'''
    keyword = 'set'
    name = 'id'
    def __init__(self, *args, **kwargs):
        self.unique = kwargs.pop('unique',False)
        self.lookup = kwargs.pop('lookup','in')
        super(QuerySet,self).__init__(*args,**kwargs)
    

class QueryQuery(QuerySet):
    keyword = 'query'
    def __iter__(self):
        return iter(())
    
    def __len__(self):
        return 0
    
    @property
    def valid(self):
        return True
    
    
class Select(QueryElement):
    """Forms the basis of select type set operations."""
    pass
    

def make_select(keyword,queries):
    data = queries[0].data.copy()
    data.update({'keyword': keyword, 'underlying': queries})
    return Select(**data)

def intersect(queries):
    return make_select('intersect',queries)

def union(queries):
    return make_select('union',queries)

def difference(queries):
    return make_select('diff',queries)

def queryset(qs):
    data = qs.data.copy()
    return QuerySet(**data)
    

class Query(Q):
    '''A :class:`Query` is produced in terms of a given :class:`Session`,
using the :meth:`Session.query` method::

    qs = session.query(MyModel)
    
A query is equivalent to a collection of SELECT statements for a standard
relational database. It has a  a generative interface whereby successive calls
return a new :class:`Query` object, a copy of the former with additional
criteria and options associated with it.

.. attribute:: _meta

    The :attr:`StdModel._meta` attribute.
    
.. attribute:: model

    the :class:`StdModel` class for this query.
    
.. attribute:: session

    The :class:`Session` which created the :class:`Query` via the
    :meth:`Session.query` method.
    
.. attribute:: backend

    the :class:`stdnet.BackendDataServer` holding the data to query.
    
.. attribute:: _get_field

    When iterating over a :class:`Query`, you get back instances of
    the :attr:`model` class. However, if ``_get_field`` is specified
    you get back values of the field specified.
    This can be changed via the :meth:`get_field` method::
    
        qs = query.get_field('name').all()
        
    the results is a list of name values (provided the model has a
    ``name`` field of course).
    
    Default: ``None``.
    
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
    lookups = ('in','contains')
    
    def __init__(self, *args, **kwargs):
        '''A :class:`Query` is not initialized directly.'''
        self.fargs  = kwargs.pop('fargs',None)
        self.eargs  = kwargs.pop('eargs',None)
        self.text  = kwargs.pop('text',None)
        super(Query,self).__init__(*args,**kwargs)
        self.clear()
     
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
            if field not in self._meta.dfields:
                raise QuerySetError('Model "{0}" has no field "{1}".'\
                                    .format(self._meta,field))
            q = self._clone()
            q.data['get_field'] = field
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
accessing the related fields of all (most) objects in your query.

:parameter fields: fields to include in the loading. If not provided all
    foreign keys and :ref:`structured fields <model-field-structure>`
    in the model will be loaded.
:rtype: a new :class:`Query`.'''
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
:meth:`Query.load_related`, since it does not reduce the number of requests
to the database. However, it can save you lots of bandwidth when excluding
data intensive fields you don't need.
'''
        q = self._clone()
        q.fields = tuple(set(self._load_only(fields))) if fields else None
        return q
    
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
        return val in self.backend_query()
    
    def delete(self):
        '''Delete all matched elements of the :class:`Query`.'''
        session = self.session
        with session.begin():
            session.delete(self)        
        #ids = self.backend_query().delete()
        #post_delete.send(sender=self.__class__, ids = ids, query = self)
    
    def construct(self):
        '''Build the :class:`QueryElement` representing this query.'''
        if self.__construct is False:
            self.__construct = self._construct()
        return self.__construct
        
    def backend_query(self, **kwargs):
        '''Build and return the :class:`stdnet.BackendQuery`.
This is a lazy method in the sense that it is evaluated once only and its
result stored for future retrieval.'''
        if self.__qset is None:
            q = self.construct()
            if q is None:
                self.__qset = EmptyQuery()
            else:
                self.__qset = q.backend_query(**kwargs)
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
        d = self.__dict__.copy()
        d['data'] = d['data'].copy()
        q.__dict__ = d
        q.clear()
        return q
    
    def clear(self):
        self.__construct = False
        self.__qset = None
        self.__slice_cache = None
        
    def cache(self):
        if not self.__slice_cache:
            self.__slice_cache = {}
        return self.__slice_cache
    
    def _construct(self):
        q = queryset(self)
        if self.fargs:
            args = []
            fargs = self.aggregate(self.fargs)
            for f in fargs:
                # no values to filter on. empty result.
                if not f.valid:
                    return
            q = intersect([q]+fargs)
        
        if self.eargs:
            eargs = self.aggregate(self.eargs)
            for a in tuple(eargs):
                if not a.valid:
                    eargs.remove(a)
            if eargs:
                if len(eargs) > 1:
                    eargs = [union(eargs)]
                q = difference([q]+eargs)
        
        return q

    def aggregate(self, kwargs):
        return sorted(self._aggregate(kwargs), key = lambda x : x.name)
        
    def _aggregate(self, kwargs):
        '''Aggregate lookup parameters.'''
        meta    = self._meta
        fields  = meta.dfields
        for name,value in kwargs.items():
            names = name.split('__')
            field_name = names[0]
            if field_name not in fields:
                raise QuerySetError('Could not filter on model "{0}".\
 Field "{1}" does not exist.'.format(meta,field_name))
            field = fields[field_name]
            if not field.index:
                raise QuerySetError("{0} {1} is not an index.\
 Cannot query.".format(field.__class__.__name__,field_name))                                 
            lookup = '__'.join(names[1:])
            if lookup:
                lvalue = field.filter(self.session, lookup, value)
                if lvalue is not None:
                    lookup = 'in'
                    value = lvalue
            else:
                lookup = 'in'
            if isinstance(value, Q):
                value = value.construct()
                query_class = QueryQuery
            else:
                query_class = QuerySet
                if not iterable(value):
                    value = (value,)
                value = tuple((field.index_value(v) for v in value))
                            
            unique = field.unique
            field_name = field.attname
            data = self.data.copy()
            data.update({'name':field.attname,
                         'underlying':value,
                         'unique':field.unique,
                         'lookup':lookup})
            yield query_class(**data)
        
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


class QueryGroup(object):
    
    def __init__(self):
        self.queries = []
        