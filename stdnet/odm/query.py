from copy import copy
from inspect import isgenerator

from stdnet import on_result, BackendRequest, range_lookups, lookup_value
from stdnet.exceptions import *
from stdnet.utils import zip, JSPLITTER, iteritems, unique_tuple

from .signals import *


__all__ = ['Q', 'Query', 'QueryElement', 'EmptyQuery',
           'intersect', 'union', 'difference']

def iterable(value):
    if isgenerator(value) or isinstance(value,(tuple,list,set,frozenset)):
        return True
    else:
        return False


def update_dictionary(result, extra):
    for k in extra:
        v = extra[k]
        if k in result:
            v2 = result[k]
            v = set(v) if iterable(v) else set((v,))
            v.update(v2) if iterable(v2) else v.add(v2)
            if len(k.split(JSPLITTER)) == 1:
                result.pop(k)
                k = k + JSPLITTER + 'in'
        result[k] = v
    return result


class SessionModelBase(object):

    def __init__(self, meta, session):
        self.meta = meta
        self.session = session

    def __repr__(self):
        return self.meta.__repr__()
    __str__ = __repr__

    @property
    def backend(self):
        return self.session.backend


class Q(object):
    '''Base class for :class:`Query` and :class:`QueryElement`.

.. attribute:: meta

    The :attr:`StdModel._meta` attribute.

.. attribute:: model

    the :class:`StdModel` class for this query.
'''
    keyword = ''
    name = ''
    def __init__(self, meta, session, select_related = None,
                 ordering = None, fields = None,
                 get_field = None, name = None, keyword = None):
        self._meta = meta
        self.session = session
        self.data = {'select_related': select_related,
                     'ordering': ordering,
                     'fields': fields,
                     'get_field': get_field}
        self.name = name if name is not None else self.name
        self.keyword = keyword if keyword is not None else self.keyword

    @property
    def meta(self):
        return self._meta

    @property
    def model(self):
        return self._meta.model

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

    def get_field(self, field):
        '''A :class:`Q` performs a series of operations and ultimately
generate of set of matched elements ``ids``. If on the other hand, a
different field is required, it can be specified with the :meth:`get_field`
method. For example, lets say a model has a field called ``object_id``
which contains ids of another model, we could use::

    qs = session.query(MyModel).get_field('object_id')

to obtain a set containing the values of matched elements ``object_id``
fields.

:parameter field: the name of the field which will be used to obtained the
    matched elements value. Must be an index.
:rtype: a new :class:`Q` instance.
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

    def __contains__(self, val):
        if isinstance(val,self.model):
            val = val.id
        return val in self.backend_query()

    def construct(self):
        raise NotImplementedError()

    def clear(self):
        pass

    def backend_query(self, **kwargs):
        '''Build the :class:`stdnet.BackendQuery` for this instance. This
is a virtual method with different implementation in :class:`Query`
and :class:`QueryElement`.'''
        raise NotImplementedError()

    def _clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        d = self.__dict__.copy()
        d['data'] = d['data'].copy()
        if self.unions:
            d['unions'] = copy(self.unions)
        q.__dict__ = d
        q.clear()
        return q


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
        underlying = kwargs.pop('underlying', None)
        super(QueryElement,self).__init__(*args, **kwargs)
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
    def executed(self):
        if self.__backend_query is not None:
            return self.__backend_query.executed
        else:
            return False

    @property
    def valid(self):
        if isinstance(self.underlying, QueryElement):
            return self.keyword == 'set'
        else:
            return len(self.underlying) > 0


class QuerySet(QueryElement):
    '''A :class:`QueryElement` which represents a lookup on a field.'''
    keyword = 'set'
    name = 'id'


class Select(QueryElement):
    """Forms the basis of select type set operations."""
    pass


def make_select(keyword,queries):
    first = queries[0]
    queries = [q.construct() for q in queries]
    return Select(first.meta, first.session, keyword = keyword,
                  underlying = queries)

def intersect(queries):
    return make_select('intersect',queries)

def union(queries):
    return make_select('union',queries)

def difference(queries):
    return make_select('diff',queries)

def queryset(qs, **kwargs):
    return QuerySet(qs._meta, qs.session, **kwargs)


class QueryBase(Q):

    def __iter__(self):
        result = self.items()
        if isinstance(result, BackendRequest):
            raise RuntimeError('Cannot iterate. Asynchronous result not ready.')
        else:
            return iter(result)

    def __len__(self):
        return self.count()

    def all(self):
        '''Return a ``list`` of all matched elements in this :class:`Query`.'''
        return self.items()


class EmptyQuery(QueryBase):
    '''Degenerate :class:`QueryBase` simulating and empty set.'''
    keyword = 'empty'
    def items(self, slic=None):
        return []

    def count(self):
        return 0

    def construct(self):
        return self

    @property
    def executed(self):
        return True

    def union(self, query, *queries):
        return query.union(*queries)

    def intersect(self, *queries):
        return self


class Query(QueryBase):
    '''A :class:`Query` is produced in terms of a given :class:`Session`,
using the :meth:`Session.query` method::

    qs = session.query(MyModel)

A query is equivalent to a collection of SELECT statements for a standard
relational database. It has a  a generative interface whereby successive calls
return a new :class:`Query` object, a copy of the former with additional
criteria and options associated with it.

**ATTRIBUTES**

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

**METHODS**
'''
    start = None
    stop = None
    lookups = ('in','contains')

    def __init__(self, *args, **kwargs):
        '''A :class:`Query` is not initialized directly but via the
:meth:`Session.query` or :meth:`Manager.query` methods.'''
        self.fargs  = kwargs.pop('fargs',None)
        self.eargs  = kwargs.pop('eargs',None)
        self.unions = kwargs.pop('unions',())
        self.intersections = kwargs.pop('intersections',())
        self.text  = kwargs.pop('text',None)
        self.exclude_fields = kwargs.pop('exclude_fields',None)
        super(Query,self).__init__(*args,**kwargs)
        self.clear()

    @property
    def executed(self):
        if self.__construct is not None:
            return self.__construct.executed
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
        if isinstance(slic, slice):
            return self.items(slic)
        return self.items()[slic]

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
                kwargs = update_dictionary(self.fargs.copy(), kwargs)
            q.fargs = kwargs
            return q
        else:
            return self

    def exclude(self, **kwargs):
        '''Returns a new :class:`Query` with additional clauses corresponding
to ``EXCEPT`` in a ``SQL SELECT`` statement.

:parameter kwargs: dictionary of limiting clauses.
:rtype: a new :class:`Query` instance.

Using an equivalent example to the :meth:`filter` method::

    qs = session.query(MyModel)
    result1 = qs.exclude(group = 'planet')
    result2 = qs.exclude(group__in = ('planet','stars'))

'''
        if kwargs:
            q = self._clone()
            if self.eargs:
                kwargs = update_dictionary(self.eargs.copy(), kwargs)
            q.eargs = kwargs
            return q
        else:
            return self

    def union(self, *queries):
        '''Return a new :class:`Query` obtained form the union of this
:class:`Query` with one or more *queries*.
For example, lets say we want to have the union
of two queries obtained from the :meth:`filter` method::

    query = session.query(MyModel)
    qs = query.filter(field1 = 'bla').union(query.filter(field2 = 'foo'))
'''
        q = self._clone()
        q.unions += queries
        return q

    def intersect(self, *queries):
        '''Return a new :class:`Query` obtained form the intersection of this
:class:`Query` with one or more *queries*. Workds the same way as
the :meth:`union` method.'''
        q = self._clone()
        q.intersections += queries
        return q

    def sort_by(self, ordering):
        '''Sort the query by the given field

:parameter ordering: a string indicating the class:`Field` name to sort by.
    If prefixed with ``-``, the sorting will be in descending order, otherwise
    in ascending order.
:return type: a new :class:`Query` instance.
'''
        if ordering:
            ordering = self._meta.get_sorting(ordering, QuerySetError)
        q = self._clone()
        q.data['ordering'] = ordering
        return q

    def search(self, text, lookup = None):
        '''Search *text* in model. A search engine needs to be installed
for this function to be available.

:parameter text: a string to search.
:return type: a new :class:`Query` instance.
'''
        if self._meta.searchengine:
            q = self._clone()
            q.text = (text,lookup)
            return q
        else:
            raise QuerySetError('Search not implemented for {0} model'\
                                .format(self.model))

    def search_queries(self, q):
        '''Return a new :class:`QueryElem` for *q* applying a text search.'''
        if self.text:
            return self._meta.searchengine.search_model(q, *self.text)
        else:
            return q

    def load_related(self, related, *related_fields):
        '''It returns a new :class:`Query` that automatically
follows the foreign-key relationship *related*.

:parameter related: A field name corresponding to a :class:`ForeignKey`
    in :attr:`Query.model`.
:parameter fields: optional :class:`Field` names for the *related*
    model to load. If not provided, all fields will be loaded.

This function is :ref:`performance boost <increase-performance>` when
accessing the related fields of all (most) objects in your query.

If Your model contains more than one foreign key, you can use this function
in a generative way::

    qs = myquery.load_related('rel1').load_related('rel2','field1','field2')

:rtype: a new :class:`Query`.'''
        meta = self._meta
        if related in meta.dfields:
            field = meta.dfields[related]
            if not hasattr(field,'relmodel'):
                raise FieldError('Load related does not apply to "{0}"'\
                                 .format(related))
        else:
            raise FieldError('Unknown field "{0}"'.format(related))
        q = self._clone()
        rf = set(related_fields)
        # if is always loaded.
        rf.discard('id')
        # we need to copy the related dictionary including its values
        if q.select_related:
            d = dict(((k,set(v)) for k,v in q.select_related.items()))
        else:
            d = {}
        q.data['select_related'] = d
        if field.name in d:
            fields = d[field.name]
            fields.update(rf)
        else:
            d[field.name] = rf
        return q

    def load_only(self, *fields):
        '''This is provides a :ref:`performance boost <increase-performance>`
in cases when you need to load a subset of fields of your model. The boost
achieved is less than the one obtained when using
:meth:`Query.load_related`, since it does not reduce the number of requests
to the database. However, it can save you lots of bandwidth when excluding
data intensive fields you don't need.
'''
        q = self._clone()
        fs = unique_tuple(q.fields, fields)
        q.data['fields'] = fs if fs else None
        return q

    def dont_load(self, *fields):
        '''Works like :meth:`load_only` to provides a
:ref:`performance boost <increase-performance>` in cases when you need
to load all fields except a subset specified by *fields*.
'''
        q = self._clone()
        fs = unique_tuple(q.exclude_fields, fields)
        q.exclude_fields = fs if fs else None
        return q

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
        return on_result(self.filter(**kwargs).items(), self._get)

    def count(self):
        '''Return the number of objects in ``self``.
This method is efficient since the :class:`Query` does not
receive any data from the server apart from the number of matched elements.
It construct the queries and count the
objects on the server side.'''
        return self.backend_query().count()

    def delete(self):
        '''Delete all matched elements of the :class:`Query`. It returns the
list of ids deleted.'''
        session = self.session
        with session.begin() as t:
            session.delete(self)
        return t.deleted.get(self._meta)

    def construct(self):
        '''Build the :class:`QueryElement` representing this query.'''
        if self.__construct is None:
            self.__construct = self._construct()
        return self.__construct

    def backend_query(self, **kwargs):
        '''Build and return the :class:`stdnet.BackendQuery`.
This is a lazy method in the sense that it is evaluated once only and its
result stored for future retrieval.'''
        q = self.construct()
        return q if isinstance(q, EmptyQuery) else q.backend_query(**kwargs)

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
    def clear(self):
        self.__construct = None
        self.__slice_cache = None

    def cache(self):
        if not self.__slice_cache:
            self.__slice_cache = {}
        return self.__slice_cache

    def _construct(self):
        if self.fargs:
            args = []
            fargs = self.aggregate(self.fargs)
            for f in fargs:
                # no values to filter on. empty result.
                if not f.valid:
                    return EmptyQuery(self._meta, self.session)
        else:
            fargs = None
        # no filters, get the whole set
        if not fargs:
            q = queryset(self)
        elif len(fargs) > 1:
            q = intersect(fargs)
        else:
            q = fargs[0]
        if self.eargs:
            eargs = self.aggregate(self.eargs)
            for a in tuple(eargs):
                if not a.valid:
                    eargs.remove(a)
            if len(eargs) > 1:
                eargs = [union(eargs)]
        else:
            eargs = None
        if eargs:
            q = difference([q]+eargs)
        if self.intersections:
            q = intersect((q,)+self.intersections)
        if self.unions:
            q = union((q,)+self.unions)
        q = self.search_queries(q)
        data = self.data.copy()
        if self.exclude_fields:
            fields = data['fields']
            if not fields:
                fields = tuple((f.name for f in self._meta.scalarfields))
            fields = tuple((f for f in fields if f not in self.exclude_fields))
            data['fields'] = fields
        q.data = data
        return q

    def aggregate(self, kwargs):
        '''Aggregate lookup parameters.'''
        meta = self._meta
        fields = meta.dfields
        field_lookups = {}
        for name, value in iteritems(kwargs):
            bits = name.split(JSPLITTER)
            field_name = bits.pop(0)
            if field_name not in fields:
                raise QuerySetError('Could not filter on model "{0}".\
 Field "{1}" does not exist.'.format(meta, field_name))
            field = fields[field_name]
            attname = field.attname
            if bits:
                bits = [n.lower() for n in bits]
                if bits[-1] == 'in':
                    bits.pop()
                lookup = JSPLITTER.join(bits)
                if lookup and lookup not in range_lookups:
                    lvalue = field.filter(self.session, lookup, value)
                    if lvalue is not None:
                        value = lvalue
                        lookup = None
                    else:
                        if bits[-1] in range_lookups:
                            lookup = bits.pop()
                        else:
                            lookup = None
                        bits.insert(0, attname)
                        attname = JSPLITTER.join(bits)
            else:
                lookup = None
            # Get lookups on attribute name
            lookups = field_lookups.get(attname)
            if lookups is None:
                lookups = []
                field_lookups[attname] = lookups
            if lookup not in range_lookups:
                if not field.index:
                    raise QuerySetError("{0} {1} is not an index.\
 Cannot query.".format(field.__class__.__name__,field_name))
                if not iterable(value):
                    value = (value,)
                for v in value:
                    if isinstance(v, Q):
                        v = lookup_value('set', v.construct())
                    else:
                        v = lookup_value('value', field.dumps(v, lookup))
                    lookups.append(v)
            else:
                lookups.append(lookup_value(lookup, field.dumps(value, lookup)))
        return [queryset(self, name=name, underlying=field_lookups[name])\
                for name in sorted(field_lookups)]

    def items(self, slic=None):
        '''Fetch data matching theis :class:`Query` and return a list
of instances of models.'''
        key = None
        seq = self.cache().get(None)
        if slic:
            if seq is not None:
                return seq[slic]
            else:
                key = (slic.start, slic.step, slic.stop)
        if seq is not None:
            return seq
        else:
            return on_result(self.backend_query().items(slic), self._items, key)
        
    def _items(self, items, key):
        if isinstance(items, Exception):
            raise items
        session = self.session
        seq = []
        model = self.model
        for el in items:
            if isinstance(el, model):
                session.add(el, modified=False)
            seq.append(el)
        self.cache()[key] = seq
        return seq

    def _get(self, items):
        if items:
            if len(items) == 1:
                return items[0]
            else:
                raise QuerySetError('Get query {0} yielded non\
 unique results'.format(self))
        else:
            raise self.model.DoesNotExist()
