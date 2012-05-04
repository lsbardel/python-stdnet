.. _tutorial-query:

.. module:: stdnet.odm

============================
Query your Data
============================

The most powerful feature of stdnet is a comprehensive API for querying your
data in an efficient and elegant way.
To retrieve objects from your data server, you construct a
:class:`Query` via a :class:`Manager` on your model class
or via a :class:`Session`.

A :class:`Query` represents a collection of objects from your model.
It can have zero, one or many filters criteria that narrow down the collection
based on given parameters.


Retrieving all objects
==========================

The simplest way to retrieve objects for a model is to get all of them.
To do this, use the :meth:`Manager.query` method on a Manager:

    >>> funds = Fund.objects.query()
    >>> funds
    Query
    >>> list(funds)
    [Fund: Markowitz]

:class:`Query` are lazy, they are evaluated only when you iterate over them
or explicitly call the :class:`Query.items` method.

Filtering
===============================
This operation is somehow equivalent to a ``SELECT WHERE`` statement in
a traditional SQL database.
To perform such operation, you refine the initial :class:`Query` by adding
filter conditions.
Lets create few other objects in the same line as above and try::

    >>> eur_funds = Fund.objects.filter(ccy = 'EUR')
    >>> eur_funds
    Query.filter({'ccy': 'EUR'})
    >>> eur_funds.count()
    1
    >>> list(eur_funds)
    [Fund: Markowitz]

The :meth:`Query.count` method counts the object in the query without
actually retrieving them.

It is possible to filter from a list/tuple of values::

    qs = Fund.objects.filter(ccy__in=('EUR','USD'))
    
This filter statement is equivalent to an union of two filters statements::

    q1 = Fund.objects.filter(ccy='EUR')
    q2 = Fund.objects.filter(ccy='USD')
    qs = q1.union(q2)
   

Concatenating
=================

You can perform further selection by concatenating queries::

    qs = Instrument.objects.filter(ccy__in=('EUR','USD')).filter(types__in=('equity',bond'))

Which is equivalent to an intersection of two filter statement:

    q1 = Fund.objects.filter(ccy__in=('EUR', 'USD'))
    q2 = Fund.objects.filter(types__in=('equity',bond'))
    qs = q1.intersect(q2)
    

Excluding
===============================    
You can also exclude fields from lookups::

    Instrument.objects.exclude(type='future')

You can exclude a list of fields::

    Instrument.objects.exclude(type__in = ('future','equity'))


Union
=======================

:meth:`Query.filter` and :meth:`Query.exclude` methods cover most common
situations. There is another method which can be used to combine together
two or more :class:`Query` into a different query. The :class:`Query.union`
method performs just that, an union of queries. Consider the following example::

    qs = Instrument.objects.filter(ccy='EUR', type='equity')
    
this retrieve all instruments with *ccy* 'EUR' AND *type* 'equity'. What about
if we need all instruments with *ccy* 'EUR' OR *type* 'equity'? We use the
:meth:`Query.union` method::

    q1 = Instruments.objecyts.filter(type = 'equity')
    qs = Instrument.objects.filter(ccy = 'EUR').union(q1)
    

.. _query_related:

Related Fields
====================

The query API goes even further by allowing to operate on
:class:`Fields` of :class:`ForeignKey` models. For example, lets consider
the :class:`Position` model in our `example application <tutorial-application>'_.
The model has a :class:`ForeignKey` to the :class:`Instrument` model.
using the related field query API one can construct a query to fetch positions
an subset of instruments in this way::

    qs = Position.objects.filter(instrument__ccy='EUR')
    
that is the name of the :class:`ForeignKey` field, followed by a double underscore (__),
followed by the name of the field in the model.

This is merely a syntactic sugar in place of this equivalent expression::

    qs = Position.objects.filter(instrument=Instrument.objects.filter(ccy='EUR'))

