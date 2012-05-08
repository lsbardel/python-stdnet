.. _tutorial:

.. module:: stdnet.odm

============================
Using Models
============================

Here we deal with model implementation and creating instances persistent
in a backend database.


.. _creating-models:

Writing Models
==========================

Defining stdnet *models* is achieved by subclassing the
:class:`StdModel` class. The following
snipped implements two models, ``Author`` and ``Book``::

    from stdnet import odm
    
    class Author(odm.StModel):
        name = odm.SymbolField()
    
    class Book(odm.StdModel):
        title  = odm.CharField()
        author = odm.ForeignKey(Author, related_name = 'books')

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

* ``Fund`` for storing information about several ``Position``.
* ``Instrument`` holds information about tradable financial intruments.
* ``Position`` is an investment on a particular ``Instrument`` in a ``Fund``.

A minimal ``stdnet`` implementation can look like this::

    from stdnet import odm
    
    class Fund(odm.StdModel):
        name = odm.SymbolField(unique = True)
        ccy = odm.SymbolField()
        description = odm.CharField()
        
        def __unicode__(self):
            return self.name
        
        
    class Instrument(odm.StdModel):
        name = odm.SymbolField(unique = True)
        ccy = odm.SymbolField()
        type = odm.SymbolField()
        prices = odm.ListField()
        
        def __unicode__(self):
            return self.name
        
        
    class Position(odm.StdModel):
        instrument = odm.ForeignKey(Instrument)
        fund = odm.ForeignKey(Fund)
        size = odm.FloatField()
        dt = odm.DateField()
        
        def __unicode__(self):
            return self.instrument

        class Meta:
            ordering = '-dt'
            
If you are familiar with django_ you will see several similarities and you should be able to understand,
with a certain degree of confidence, what it is going on.
The only difference is the ``prices`` :class:`ListField`
in the ``Instrument`` model which is
not available in a traditional relational database.

The metaclass
~~~~~~~~~~~~~~~~~~~~~~~
The ``Position`` models specifies a ``Meta`` class with an ``ordering``
attribute.
When provided, as in this case, the Meta class fields are used by the ``odm``
to customise the build of the :class:`Metaclass` for the model. The metaclas
is stored in the :attr:`StdModel._meta` attribute.

In this case we instruct the ``odm`` to manage the ``Position`` model
as ordered with respect to the :class:`DateField` ``dt``
in descending order. Check the  :ref:`sorting <sorting>`
documentation for more details or ordering and sorting.


Registering Models
================================

Before playing with the API let's to :ref:`register the models <register-model>`
to a backend server. Registration is not compulsory, but it is required when using
model's :class:`Manager`::

    import odm

    odm.register(Fund, 'redis://my.host.name:6379/?db=1')
    odm.register(Instrument, 'redis://my.host.name:6379/?db=1')
    odm.register(Position, 'redis://my.host.name:6379/?db=1')
    

.. _one-to-many:

One-to-many relationships
================================

The *Position* model contains two :class:`ForeignKey` fields.
In the context of relational databases a
`foreign key <http://en.wikipedia.org/wiki/Foreign_key>`_ is
a referential constraint between two tables.

For stdnet is exactly the same thing. The field store the ``id`` of a
related :class:`StdModel` instance.
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

An instance of a :class:`StdModel`, an object for clarity,
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


.. _django: https://www.djangoproject.com/
.. _descriptors: http://users.rcn.com/python/download/Descriptor.htm