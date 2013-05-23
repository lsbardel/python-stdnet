.. _model-model:

.. module:: stdnet.odm

============================
Model and Query API
============================

The *object-data mapper* (ODM) is the core of the library. It defines an API for mapping
data in the backend key-value store to objects in Python.
It'is name is closely related to
`object relational Mapping <http://en.wikipedia.org/wiki/Object-relational_mapping>`_ (ORM),
a programming technique for converting data between incompatible
type systems in traditional `relational databases <http://en.wikipedia.org/wiki/Relational_database>`_
and object-oriented programming languages.


Model
==================

The object data mapper presents a method of
associating user-defined Python classes, referred as **models**,
with data in a :class:`stdnet.BackendDataServer`.
These python classes are subclasses of
:class:`stdnet.odm.StdModel`.


StdModel Class
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: StdModel
   :members:
   :member-order: bysource


Model
~~~~~~~~~~~~~~~~~

.. autoclass:: Model
   :members:
   :member-order: bysource


Create Model
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: create_model


.. _database-metaclass:

Model Meta
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ModelMeta
   :members:
   :member-order: bysource
   

Model State
~~~~~~~~~~~~~~~~~

.. autoclass:: ModelState
   :members:
   :member-order: bysource
   
   
autoincrement
~~~~~~~~~~~~~~~~~~

.. autoclass:: autoincrement
   :members:
   :member-order: bysource


Queries
================

Query base class
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Q
   :members:
   :member-order: bysource


.. _model-query:

Query
~~~~~~~~~~~~~~~

.. autoclass:: Query
   :members:
   :member-order: bysource

   .. automethod:: __init__

QueryElement
~~~~~~~~~~~~~~~

.. autoclass:: QueryElement
   :members:
   :member-order: bysource


SearchEngine Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: SearchEngine
   :members:
   :member-order: bysource


.. _model-structures:

Data Structures
==============================

Data structures are subclasses of :class:`Structure`, which in term is
a subclass of :class:`Model`. They are rarely used in stand alone mode,
instead they form the back-end of
:ref:`data structure fields <model-field-structure>`.

There are five of them:

 * :class:`List`, implemented as a doubly-linked sequence.
 * :class:`Set`, a container of unique values.
 * :class:`HashTable`, unique associative container.
 * :class:`Zset`, an ordered container of unique values.
 * :class:`TS`, a time-series implemented as a ordered unique associative container.

An additional structure is provided in the :mod:`stdnet.apps.columnts` module

 * :class:`stdnet.apps.columnts.ColumnTS` a numeric multivariate time-series structure
   (useful for modelling financial data for example).

.. note::

    Stand alone data structures are available for redis back-end only. Usually,
    one uses these models via a
    :ref:`data-structure fields <model-field-structure>`.
    
    
Creating Structures
~~~~~~~~~~~~~~~~~~~~~~~

Creating the five structures available in stdnet is accomplished
in the following way::

    from stdnet import odm

    models = odm.Router('redis://localhost:6379')
    li = models.register(odm.List())
    
At this point the ``li`` instance is registered with a :class:`Router` and the
session API can be used::

    with models.session().begin() as t:
        t.add(li)
        li.push_back('bla')
        li.push_back('foo')

If no ``id`` is specified, stdnet will create one for you::

    >>> l.id
    '2d0cbac9'


Base Class and Mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Structure
   :members:
   :member-order: bysource

.. autoclass:: Sequence
   :members:
   :member-order: bysource

.. autoclass:: PairMixin
   :members:
   :member-order: bysource

.. autoclass:: KeyValueMixin
   :members:
   :member-order: bysource

.. autoclass:: OrderedMixin
   :members:
   :member-order: bysource

.. autoclass:: StructureCache
   :members:
   :member-order: bysource


List
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: List
   :members:
   :member-order: bysource


Set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Set
   :members:
   :member-order: bysource


.. _orderedset-structure:

OrderedSet
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Zset
   :members:
   :member-order: bysource


.. _hash-structure:

HashTable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: HashTable
   :members:
   :member-order: bysource


TS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TS
   :members:
   :member-order: bysource


Sessions
=========================

A :class:`Session` is the middleware between a :class:`Manager` and
a :class:`stdnet.BackendDataServer`. It is obtained from either the
:meth:`Router.session` or, equivalently, from the :meth:`Manager.session`.
A :class:`Session` is an holding zone for :class:`SessionModel` and it
communicates with the :class:`stdnet.BackendDataServer` via :class:`Transaction`.

Session
~~~~~~~~~~~~~~~

.. autoclass:: Session
   :members:
   :member-order: bysource
   
Session Model
~~~~~~~~~~~~~~~

.. autoclass:: SessionModel
   :members:
   :member-order: bysource
   
Transaction
~~~~~~~~~~~~~~~

.. autoclass:: Transaction
   :members:
   :member-order: bysource
   
Managers
=================

Manager
~~~~~~~~~~~~~~~~~~
.. autoclass:: Manager
   :members:
   :member-order: bysource
   
   
RelatedManager
~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.odm.related.RelatedManager
   :members:
   :member-order: bysource
   
One2ManyRelatedManager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.odm.related.One2ManyRelatedManager
   :members:
   :member-order: bysource
   
   
Many2ManyRelatedManager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.odm.related.Many2ManyRelatedManager
   :members:
   :member-order: bysource

LazyProxy
~~~~~~~~~~~~~~~~~~

.. autoclass:: LazyProxy
   :members:
   :member-order: bysource
     
.. _register-model:


Registration
======================

To interact with a :class:`stdnet.BackendDataServer`,
Models must registered. Registration is obtained via a :class:`Router` which
has two methods for registering models. The first one is the :meth:`Router.register`
method which is used to register a model and, possibly, all its related
models. The second method is the :meth:`Router.register_applications` which
registers all :class:`Model` from a list of python dotted paths or
python modules.

Check the :ref:`registering models tutorial <tutorial-registration>`
for further explanation and examples.

Router
~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.odm.Router
   :members:
   :member-order: bysource



.. _standard template library: http://www.sgi.com/tech/stl/
.. _SQLAlchemy: http://www.sqlalchemy.org/   
.. _Django: http://docs.djangoproject.com/en/dev/ref/models/instances/