.. _tutorial-query:

.. module:: stdnet.orm


============================
Query your data
============================

The most powerful feature of stdnet is a comprehensive query API for your
data.

To retrieve objects from your data server, you construct a
:class:`Query` via a :class:`Manager` on your model class
or via a :class:`Session`.

A :class:`stdnet.orm.Query` represents a collection of objects from your
database.
It can have zero, one or many filters criteria that narrow down the collection
based on given parameters.

You get a :class:`Query` by using your model's Manager. Each model has at least one Manager,
and it's called objects by default. Access it directly via the model class::

    >>> Fund.objects
    <stdnet.orm.query.Manager object at ...>
    >>>

Retrieving all objects
==========================

The simplest way to retrieve objects from a table is to get all of them. To do this, use the :meth:`stdnet.orm.query.Manager.all`
method on a Manager:

    >>> funds = Fund.objects.query()
    >>> funds
    Query
    >>> list(funds)
    [Fund: Markowitz]

:class:`stdnet.orm.Query` are lazy, they are evaluated only when you iterate over them.
The results are then stored in the ``_seq`` attribute.

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

The ``count`` method counts the object in the query without physically retrieving them.


Retrieving from a list::

    Fund.objects.filter(ccy__in = ('EUR','USD'))
    
   
You can perform further section by concatenating queries::

    Instrument.objects.filter(ccy__in = ('EUR','USD')).filter(types__in = ('equity',bond'))



Excluding
===============================    
You can also exclude fields from lookups::

    Instrument.objects.exclude(type = 'future')
    
and so forth.


