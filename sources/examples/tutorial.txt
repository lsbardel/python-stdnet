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


.. _tutorial-models-router:

Registering Models
================================

Before playing with the API, we need to register the models we intend to use
with back-end servers. :ref:`Registration <tutorial-registration>` is an important
topic and is covered in details in the next tutorial. For now,
all we need to know is that it provides a placeholder for :class:`Model`
and their :ref:`backend server <db-index>`. The placeholder is given by
the :class:`Router` which is a mapping from a :class:`Model`
to a :class:`Manager`::

    import odm

    models = odm.Router('redis://my.host.name:6379?db=1')
    
    models.register(Fund)
    models.register(Instrument)
    models.register(Position)

The above code registers the three models to a :ref:`redis backend <redis-server>`,
at redis db 1. You can pass several parameters to the connection string, including a ``password``,
a connection ``timeout`` and a ``namespace`` for your model keys. For example::

    router.register(Fund, 'redis://my.host.name:6379?db=3&password=pippo&namespace=xxxx.&timeout=5')

includes all possible parameters for a :ref:`redis connection string <redis-connection-string>`.
The :ref:`registration tutorial <tutorial-registration>` illustrates the different ways
one can register models and how to organise your application.

Creating objects
==================

Using the `models` :class:`Router` registered above, is equivalent to executing
queries in the backend database.
Once you've created your models, ``stdnet`` automatically gives you
a data-server abstraction API that lets you create, retrieve,
update and delete objects.

An instance of a :class:`StdModel`, an object for clarity,
is created by initialising it using keyword arguments which match
model's :class:`Field`. Here's an example::

    >>> b = models[Fund](name='Markowitz', ccy='EUR')
    >>> b
    Fund: Markowitz
    >>> b.id
    >>> b.name
    'Markowitz'
    >>> b.ccy
    'EUR'
    >>> b.description
    ''
    
The instance created in this way is not persistent in the backend server, and you can
check that by noting that its ``id`` is ``None``.
To commit changes, one obtains a :class:`Session` and add the instance to it::

    >>> models.session().add(b)
    >>> b.id
    1
    >>> b.name
    'Markowitz'
    >>> b.ccy
    'EUR'
    >>> b.description
    ''

A shortcut for creating an instance is to use the :meth:`Manager.new` method::

    >>> b = models['Fund'].new(name='Pluto', ccy='EUR')
    >>> b.id
    2
    

An alternative interface for accessing a model :class:`Manager` is the
:ref:`router dot notation <router-dotted>`::

    >>> b = models.fund.new(name='Star', ccy='GBP')
    >>> b
    Fund: Star
    >>> b.id
    3
    
    
For bulk updates it is better to use the :ref:`session api <model-session>`
which in this example would look like::

    >>> with models.session().begin() as transaction:
    >>>     transaction.add(models.fund(name='Markowitz', ccy='EUR'))
    >>>     transaction.add(Fund(name='Pluto', ccy='USD'))
    >>>     ...
    >>>
    
On exit of the ``with`` block, the transaction commits changes to the server.
    
.. _django: https://www.djangoproject.com/
