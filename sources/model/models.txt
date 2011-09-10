.. _model-model:

.. module:: stdnet.orm.models

============================
Model and Query API
============================

Model
==================

The StdNet Object Relational Mapper presents a method of
associating user-defined Python classes, referred as **models**,
with data in a :class:`stdnet.BackendDataServer`.
These python classes are subclasses of
:class:`stdnet.orm.StdModel`.


StdModel Class
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.StdModel
   :members:
   :member-order: bysource

 
.. _database-metaclass:

Data Server Metaclass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.base.Metaclass
   :members:
   :member-order: bysource


        
.. _register-model:
        
Registration
======================

Once a model is defined, in order to use it in an application it needs to be
registered with a back-end database.

Register
~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.register


Register application models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.register_application_models  


Register applications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.register_applications

   
Query
==============================


QuerySet
~~~~~~~~~~~~~~~
Though you usually won't create one manually, you'll go through a :class:`stdnet.orm.query.Manager`,
here's the formal declaration of a QuerySet.

.. autoclass:: stdnet.orm.query.QuerySet
   :members:
   :member-order: bysource
   

Manager
~~~~~~~~~~~~~~~
.. autoclass:: stdnet.orm.query.Manager
   :members:
   :member-order: bysource
   

Search Engine
=====================
.. autoclass:: stdnet.orm.SearchEngine
   :members:
   :member-order: bysource

.. _signal-api:

Signals
=====================
Stdnet includes a signal dispatcher which helps allow decoupled
applications get notified when actions occur elsewhere in the framework.
In a nutshell, signals allow certain senders to notify a set of receivers
that some action has taken place.
They’re especially useful when many pieces of code may be interested in
the same events.