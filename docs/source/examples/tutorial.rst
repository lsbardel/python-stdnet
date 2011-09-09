.. _tutorial:


============================
Tutorial
============================

In this section we will walk you though all the main aspects of the library,
following a simple application as example.
We will refer back to the :ref:`library API <model-index>`
as much as possible so that
advanced configuration parameters and functionalities can be investigated.


.. _creating-models:

Creating Models
==========================

Defining stdnet *models* is achieved by subclassing the
:class:`stdnet.orm.StdModel` class. The following
snipped implements two models, ``Author`` and ``Book``::

    from stdnet import orm
    
    class Author(orm.StModel):
        name = orm.SymbolField()
    
    class Book(orm.StdModel):
        title  = orm.CharField()
        author = orm.ForeignKey(Author, related_name = 'books')

The API should look familiar if you have come across django_
web framework. :ref:`Fields <model-field>` (name in the Author model,
title and author in the Book model) are specified as attribute of models.
But while fields in django are the python representation of the columns in the
backend database table, fields in stdnet are the fields of a redis hash table
which represents an instance of a model.

Information is available regarding how models instances are mapped into
:ref:`the redis backend <redis-backend>`.
 

An application
~~~~~~~~~~~~~~~~~~~~~~

Let's start with tutorial application: a small hedge fund.
You never know it may become useful in the future!

The application uses the following three models::

    from stdnet import orm
    
    class Fund(orm.StdModel):
        name = orm.SymbolField(unique = True)
        ccy = orm.SymbolField()
        description = orm.CharField()
        
        def __unicode__(self):
            return self.name
        
        
    class Instrument(orm.StdModel):
        name = orm.SymbolField(unique = True)
        ccy = orm.SymbolField()
        type = orm.SymbolField()
        prices = orm.ListField()
        
        def __unicode__(self):
            return self.name
        
        
    class Position(orm.StdModel):
        instrument = orm.ForeignKey(Instrument)
        fund = orm.ForeignKey(Fund)
        size = orm.FloatField()
        dt = orm.DateField()
        
        def __unicode__(self):
            return self.instrument

        class Meta:
            ordering = '-dt'
            
If you are familiar with django_ you will see several similarities and you should be able to understand,
with a certain degree of confidence, what it is going on.
The only difference is the ``prices`` :class:`stdnet.orm.ListField`
in the ``Instrument`` model which is
not available in a traditional relational database.


Registering Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before playing with the API you need to :ref:`register the models <register-model>`::

    import orm

    orm.register(Fund, 'redis://my.host.name:6379/?db=1')
    orm.register(Instrument, 'redis://my.host.name:6379/?db=1')
    orm.register(Position, 'redis://my.host.name:6379/?db=2')
    

        
Using Models
==================

Using models is equivalent to executing queries to the backend database.
Once again, the API is very similar to executing queries in django_.
Once you've created your models, ``stdnet`` automatically gives you
a data-server abstraction API that lets you create, retrieve,
update and delete objects. 

Creating objects
~~~~~~~~~~~~~~~~~~~~~

An instance of a :class:`stdnet.orm.StdModel`, an object for clarity,
is mapped to a hash table in the :class:`stdnet.BackendDataServer` backend.
To create an object, instantiate it using keyword arguments to the
model class, then call ``save()`` to save it to the data-server.
Here's an example::

	>>> b = Fund(name='Markowitz', ccy='EUR')
	>>> b.save()
	Fund: Markowitz
	>>> b.id
	1
	>>> b.name
	'Markowitz'
	>>> b.ccy
	'EUR'
	>>> b.description
	''

	
Retrieving objects
~~~~~~~~~~~~~~~~~~~~~~~~~

To retrieve objects from your data server, you construct a :class:`stdnet.orm.query.QuerySet`
via a :class:`stdnet.orm.query.Manager` on your model class.

A QuerySet represents a collection of objects from your database.
It can have zero, one or many filters criteria that narrow down the collection
based on given parameters.

You get a QuerySet by using your model's Manager. Each model has at least one Manager,
and it's called objects by default. Access it directly via the model class::

	>>> Fund.objects
	<stdnet.orm.query.Manager object at ...>
	>>>

Retrieving all objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~
The simplest way to retrieve objects from a table is to get all of them. To do this, use the :meth:`stdnet.orm.query.Manager.all`
method on a Manager:

	>>> funds = Fund.objects.all()
	>>> funds
	QuerySet
	>>> funds._seq
	>>> list(funds)
	[Fund: Markowitz]
	>>> funds._seq
	[Fund: Markowitz]

QuerySet are lazy, they are evaluated only when you iterate over them.
The results are then stored in the ``_seq`` attribute.

Retrieving filtered objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Usually, you'll need to select only a subset of the complete set of objects.
To create such a subset, you refine the initial QuerySet, adding filter conditions.
Lets create few other objects in the same line as above and try::

	>>> eur_funds = Fund.objects.filter(ccy = 'EUR')
	>>> eur_funds
	QuerySet.filter({'ccy': 'EUR'})
	>>> eur_funds.count()
	1
	>>> list(eur_funds)
	[Fund: Markowitz]

The ``count`` method counts the object in the query without physically retrieving them.


Retrieving from a list (equivalent to a select where in SQL)::

	Fund.objects.filter(ccy__in = ('EUR','USD'))
	
	
Concatenating queries::

	Instrument.objects.filter(ccy__in = ('EUR','USD')).filter(types__in = ('equity',bond'))
	
You can also exclude fields from lookups::

	Instrument.objects.exclude(type = 'future')
	
and so forth. The API is very similar to django_, but it is for an unstructured-in memory database.


.. _sorting:

Sorting
==================
Since version 0.6.0, stdnet provides sorting using two different ways:

* Explicit sorting using the :attr:`stdnet.orm.query.QuerySet.sort_by` attribute
  of a queryset.
* Implicit sorting via the :attr:`stdnet.orm.Meta.ordering` attribute of
  the model metaclass.


Explicit Sorting
~~~~~~~~~~~~~~~~~~~~

Sorting is usually achieved by using the :meth:`stdnet.orm.query.QuerySet.sort_by`
method with a field name as parameter. Lets consider the following model::

    class SportActivity(orm.StdNet):
        person = orm.SymbolField()
        activity = orm.SymbolField()
        dt = orm.DateTimeField()
        

To obtained a sorted query on dates for a given person::

    SportActivity.objects.filter(person='pippo').sort_by('-dt')

The negative sign in front of ``dt`` indicates descending order.


Implicit Sorting
~~~~~~~~~~~~~~~~~~~~

Implicit sorting is achieved by setting the ``ordering`` attribute in the model Meta class.
Let's consider the following Log model example::

    class Log(orm.StdModel):
        '''A database log entry'''
        timestamp = orm.DateTimeField(default=datetime.now)
        level = orm.SymbolField()
        msg = orm.CharField()
        source = orm.CharField()
        host = orm.CharField()
        user = orm.SymbolField(required=False)
        client = orm.CharField()
    
        class Meta:
            ordering = '-timestamp'

It makes lots of sense to have the log entries always sorted in a descending
order with respect to the ``timestamp`` field.
This solution always returns querysets in this order, without the need to
call ``sort_by`` method.

.. note:: Implicit sorting is a much faster solution than explicit sorting,
          since there is no sorting step involved (which is a ``N log(N)``
          time complexity algorithm). Instead, the order is maintained by using
          sorted sets as indices rather than sets.


.. _model-transactions:

Transactions
==========================

Since version 0.5.6, stdnet perform server updates via transactions.
Transaction are important for two reasons:

* To guarantee atomicity and therefore consistency of model instances when updating/deleting.
* To speed up updating/deleting of several instances at once.

A tipical usage to speed up the creation of several instances of a model ``MyModel``::

    with MyModel.transaction() as t:
        for kwargs in data:
            MyModel(**kwargs).save(t)


   
.. _django: http://www.djangoproject.com/