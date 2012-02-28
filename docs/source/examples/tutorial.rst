.. _tutorial:


============================
Using Models
============================

Here we deal with model implementation and creating instances persistent
in a backend database.


.. _creating-models:

Writing Models
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
backend database table, fields in stdnet are stored in different ways, depending
on the :ref:`backend <db-index>` used.
For example, in :ref:`redis <redis-server>`, fields are equivalent to the
fields of a redis hash table which represents an instance of a model.
 

.. _tutorial-application:

An application
======================

Let's start with tutorial application: a small hedge fund.
You never know it may become useful in the future!

The application uses the following three models,

* A ``Fund`` which stores information about several ``Position``
* A ``Position`` is an investment on a particular funancial ``Instrument``.

A minimal ``stdnet`` implementation can look like this::

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
================================

Before playing with the API you need to :ref:`register the models <register-model>`::

    import orm

    orm.register(Fund, 'redis://my.host.name:6379/?db=1')
    orm.register(Instrument, 'redis://my.host.name:6379/?db=1')
    orm.register(Position, 'redis://my.host.name:6379/?db=2')
    

.. _one-to-many:

One-to-many relationships
================================

The *Position* model contains two :class:`stdnet.orm.ForeignKey` fields.
In the context of relational databases a
`foreign key <http://en.wikipedia.org/wiki/Foreign_key>`_ is
a referential constraint between two tables.

For stdnet is exactly the same thing. The field store the ``id`` of a
related :class:`stdnet.orm.StdModel` instance.
Behind the scenes, this functionality is implemented by Python descriptors_.
This shouldn't really matter to you, but we point it out here for the curious.

        
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

.. _implicit-sorting:

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

   
.. _django: http://www.djangoproject.com/