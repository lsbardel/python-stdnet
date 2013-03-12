.. _tutorial:

.. module:: stdnet.odm

============================
Using Models
============================

Here we deal with model implementation and creating instances persistent
in a backend database. A :class:`Model` is the single, definitive source of
data about your data. There are two types of models in :mod:`stdnet`:
the :class:`StdModel` which consists of :class:`Field` and behaviours of
the data you are storing and the
:class:`Structure`, the networked equivalent of data-structures such as sets,
hash tables, lists. The goal is to define your data model in one place
and automatically derive things from it.

In this tutorial we concentrate on :class:`StdModel`, which can be thought as the
equivalent to a table in a conventional relational database. The data structures will
be covered in the :ref:`using data structures <tutorial-structures>` tutorial.

.. _creating-models:

Writing Models
==========================

Defining stdnet *models* is achieved by subclassing the
:class:`StdModel` class. The following
snippet implements two models, ``Author`` and ``Book``::

    from stdnet import odm

    class Author(odm.StdModel):
        name = odm.SymbolField()

    class Book(odm.StdModel):
        title = odm.CharField()
        author = odm.ForeignKey(Author, related_name='books')

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

Let's start with the tutorial application: a small hedge fund.
You never know it may become useful in the future!
The application uses the following three models,

* ``Fund`` for storing information about several ``Position``.
* ``Instrument`` holds information about tradable financial intruments.
* ``Position`` is an investment on a particular ``Instrument`` in a ``Fund``.

A minimal :mod:`stdnet` implementation can look like this::

    from stdnet import odm

    class Fund(odm.StdModel):
        name = odm.SymbolField(unique=True)
        ccy = odm.SymbolField()
        description = odm.CharField()

        def __unicode__(self):
            return self.name


    class Instrument(odm.StdModel):
        name = odm.SymbolField(unique=True)
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

.. note::    **The metaclass**
    
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

Before playing with the API, we :ref:`register models <register-model>`
with a backend server. Registration is not compulsory, but it is required when
using model's :class:`Manager`::

    import odm

    odm.register(Fund, 'redis://my.host.name:6379?db=1')
    odm.register(Instrument, 'redis://my.host.name:6379?db=1')
    odm.register(Position, 'redis://my.host.name:6379?db=1')

The above code registers the three models to a redis backend, at redis db 1.
You can pass several parameters to the connection string, including a ``password``,
a connection ``timeout`` and a ``namespace`` for your model keys. For example::

    odm.register(Fund, 'redis://my.host.name:6379?db=3&password=pippo&namespace=xxxx.&timeout=5')

includes all possible parameters for a redis backend.

Creating objects
==================

Using models is equivalent to executing queries in the backend database.
Once you've created your models, ``stdnet`` automatically gives you
a data-server abstraction API that lets you create, retrieve,
update and delete objects.

An instance of a :class:`StdModel`, an object for clarity,
is created by initialising it using keyword arguments which match
model's :class:`Field` names and then call the :class:`Model.save` method
to commit it changes to the data-server. Here's an example::

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
