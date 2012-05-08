.. _model-model:

.. module:: stdnet.odm

============================
Model and Query API
============================


Model and Query
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

 
 
.. _database-metaclass:

Data Server Metaclass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Metaclass
   :members:
   :member-order: bysource

Model
~~~~~~~~~~~~~~~~~

.. autoclass:: Model
   :members:
   :member-order: bysource

.. _model-query:

Query
~~~~~~~~~~~~~~~

.. autoclass:: Query
   :members:
   :member-order: bysource
   
   .. automethod:: __init__


autoincrement
~~~~~~~~~~~~~~~~~~

.. autoclass:: autoincrement
   :members:
   :member-order: bysource
   

.. _model-structures:

Data Structures
==============================

Data structures are fundamental constructs around which you build your application.
They are used in almost all computer programs, therefore becoming fluent in what the standard
data-structures can do for you is essential to get full value out of them.
The `standard template library`_ in C++ implements a wide array of structures,
python has several of them too. ``stdnet`` implements **five** remote structures:
list, set, ordered set, hash and timeseries.

The structures are bind to a remote dataserver and they derive from
from :class:`stdnet.Structure` base class.


Creating Structures
~~~~~~~~~~~~~~~~~~~~~~~

Creating the five structures available in stdnet is accomplished 
in the following way::
    
    from stdnet import odm

    session = odm.Session(...)
    l = session.add(odm.List())
    s = session.add(odm.Set())
    o = session.add(odm.Zset())
    h = session.add(odm.HashTable())
    t = session.add(odm.TS())
    
If no ``id`` is specified, stdnet will create one for you::

    >>> l.id
    '2d0cbac9'
    >>>
    
To add data you have two options: immediate commit or transactions. For example,
lets add elements to a set::
    
    >>> s.update((4,6,'bla',foo',4))
    >>> s.size()
    4
    
 or we could use transactions to combine several updates together.
 To use a transaction we do::
 
    with session.begin():
        s.update((4,6,'bla',foo',4))
        h['foo'] = 56
        o.add(3,'a zset element with score 3')
    
        
Structure Base Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Structure
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
        

.. _register-model:


Registration
======================

Models can be registered with a :class:`stdnet.BackendDataServer` so that
the model :class:`Manager` can be used to create instance and query the database.
If a model is not registered, the only way to operate on it is via the
:class:`Session` API. 

Stdnet provides two registration functions, a low level and a higher level one
which can be used to register several models at once.

Register
~~~~~~~~~~~~~~~~

.. autofunction:: register


Register application models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: register_application_models  


Register applications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: register_applications


Registered models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: registered_models


Unregister model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: unregister

.. _signal-api:

Signals
=====================
Stdnet includes a signal dispatcher which helps allow decoupled
applications get notified when actions occur elsewhere in the framework.
In a nutshell, signals allow certain senders to notify a set of receivers
that some action has taken place.
They’re especially useful when many pieces of code may be interested in
the same events.

.. _standard template library: http://www.sgi.com/tech/stl/