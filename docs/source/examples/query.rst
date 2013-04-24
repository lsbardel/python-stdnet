.. _tutorial-query:

.. module:: stdnet.odm

============================
Query your Data
============================

The most powerful feature of :mod:`stdnet` is a comprehensive API for querying,
searching and sorting your data in an efficient and elegant way.
To retrieve objects from your data server, you construct a
:class:`Query` via a :class:`Manager` on your model class
or via a :class:`Session`.
A :class:`Query` represents a collection of rules to aggregate objects
from your model.

In the following tutorial, it is assumed the models are registered with a
backend dataserver so that the default :class:`Manager` is available.
Alternatively, one can use the :ref:`session api <model-session>`.


Retrieving all objects
==========================

The simplest way to retrieve objects for a model is to get all of them.
To do this, use the :meth:`Manager.query` method on a Manager:

    >>> funds = Fund.objects.query()
    >>> funds
    Query
    >>> list(funds)
    [Fund: Markowitz]

.. note::

    :class:`Query` are lazy, they are evaluated only when you iterate over them,
    or invoke the the :class:`Query.all` method or slice the query like a
    list.

.. _tutorial-filter:

Filtering
===============================
This operation is somehow equivalent to a ``SELECT WHERE`` statement in
a traditional SQL database.
To perform such operation, you refine the initial :class:`Query` by adding
filter conditions.
Lets create few other objects in the same line as above and try::

    >>> eur_funds = Fund.objects.filter(ccy='EUR')
    >>> eur_funds
    Query.filter({'ccy': 'EUR'})
    >>> eur_funds.count()
    1
    >>> list(eur_funds)
    [Fund: Markowitz]

The :meth:`Query.count` method counts the object in the query without
actually retrieving them. It is possible to filter from a list/tuple of values::

    qs = Fund.objects.filter(ccy=('EUR','USD'))

This filter statement is equivalent to an union of two filters statements::

    q1 = Fund.objects.filter(ccy='EUR')
    q2 = Fund.objects.filter(ccy='USD')
    qs = q1.union(q2)


Concatenating filters
========================

You can perform further selection by concatenating filters::

    qs = Instrument.objects.filter(ccy=('EUR','USD')).filter(types=('equity',bond'))
    
or equivalently::
    
    qs = Instrument.objects.filter(ccy=('EUR','USD'), types=('equity',bond'))

Which is equivalent to an **intersection** of two filter statement::

    q1 = Fund.objects.filter(ccy=('EUR', 'USD'))
    q2 = Fund.objects.filter(types=('equity',bond'))
    qs = q1.intersect(q2)


.. _tutorial-exclude:

Exclude
===============================
You can also exclude fields from lookups::

    Instrument.objects.exclude(type='future')

You can exclude a list of fields::

    Instrument.objects.exclude(type=('future','equity'))

Concatenation is also supported::

    qs = Instrument.objects.exclude(ccy=('EUR','USD'), types=('equity',bond'))


Union
=======================

:meth:`Query.filter` and :meth:`Query.exclude` methods cover most common
situations. There is another method which can be used to combine together
two or more :class:`Query` into a different query. The :meth:`Query.union`
method performs just that, an union of queries. Consider the following example::

    qs = Instrument.objects.filter(ccy='EUR', type='equity')

this retrieve all instruments with ``ccy='EUR'`` **AND** ``type='equity'``. What about
if we need all instruments with ``ccy='EUR'`` **OR** ``type='equity'``? We use the
:meth:`Query.union` method::

    q1 = Instruments.objecyts.filter(type='equity')
    qs = Instrument.objects.filter(ccy='EUR').union(q1)


.. _range-lookups:

Range lookups
====================

Range lookups is how you refine the query methods you have learned so far.
They are specified by appending a suffix to the field name preceded by double underscore ``__``. 
Range lookups can be applied to any :class:`Field` which has an internal
numerical representation. Such fields
are: :class:`IntegerField`, :class:`FloatField`, :class:`DateField`, :class:`DateTimeField` and so on.   

There are four of them:

 * ``gt``, greater than. For example::
    
    qs = Position.objects.filter(size__gt=100)
    
 * ``ge``, greater than or equal to. For example::
    
    qs = Position.objects.filter(size__ge=100)
    
 * ``lt``, less than. For example::
    
    qs = Position.objects.filter(size__lt=100)
    
 * ``le``, less than or equal to. For example::
    
    qs = Position.objects.filter(size__le=100)    
 
 
They can be combined, for example, this is a :class:`Query` for a ``size`` between
10 and 100::

    qs = Position.objects.filter(size__ge=10, size__le=100)
     

.. _text-lookups:

Text lookups
====================

Text lookups is the :ref:`range lookup <range-lookups>` for text fields
such as :class:`SymbolField`, :class:`CharField` and :class:`JSONField`.

There are four of them:

 * ``contains``, check if a text field contains the text. For example::
    
    qs = Fund.objects.filter(description__contains='technology')
    
 * ``startswith``, check if a text field starts with the given text. For example::
    
    qs = Fund.objects.filter(description__starts='The')
    
 * ``endswith``, check if a text field ends with the given text. For example::
    
    qs = Fund.objects.filter(description__endswith='The')
    

.. _query_where:
    
Where
===========

Use the :meth:`Query.where` method to pass a string containing a valid 
**expression** to the query system to provide greater flexibility with queries.
Consider the following model::

    class Data(odm.StdModel):
        flag = odm.CharField()
        a = odm.FloatField()
        b = odm.FloatField()
        ...

The following is a query which works for both mongo and redis::

    qs = Data.objects.query().where('this.a > this.b')

The where method can be chained in the same way as
:ref:`filter <tutorial-filter>` and :ref:`exclude <tutorial-exclude>`::

    s = Data.objects.filter(flag='foo', a__lt=4).where('this.a > this.b')

.. note::
    
    The expression evaluates to lua in redis and to javascript in mongo.

    
.. _query_related:

Related Fields
====================

The query API goes even further by allowing to operate on
:class:`Fields` of :class:`ForeignKey` models. For example, lets consider
the :class:`Position` model in our :ref:`example application <tutorial-application>`.
The model has a :class:`ForeignKey` to the :class:`Instrument` model.

Using the related field query API one can construct a query to fetch positions
an subset of instruments in this way::

    qs = Position.objects.filter(instrument__ccy='EUR')

that is the name of the :class:`ForeignKey` field, followed by a double underscore ``__``,
followed by the name of the field in the related model.

This is merely a syntactic sugar in place of this equivalent query::

    qi = Instrument.objects.filter(ccy='EUR')
    qs = Position.objects.filter(instrument=qi)
    
    
.. _query_slice:

Limit Query Size
====================

When dealing with large amount of data, a :class:`Query` can be sliced
using Python�s array-slicing syntax. For example, this returns the first
10 objects::

    >> Instrument.objects.query()[:10]
    
This returns the sixth through tenth objects::

    >> Instrument.objects.query()[5:10]
    
This returns the last 5 objects::

    >> Instrument.objects.query()[-5:]