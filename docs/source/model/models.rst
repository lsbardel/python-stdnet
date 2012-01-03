.. _model-model:

.. module:: stdnet.orm

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

.. autoclass:: StdModel
   :members:
   :member-order: bysource

 
 
.. _database-metaclass:

Data Server Metaclass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Metaclass
   :members:
   :member-order: bysource

        
.. _register-model:
        
Registration
======================

Once a model is defined, in order to use it in an application it needs to be
registered with a back-end database. Unregistered models cannot be used to save
data and they will raise exceptions.

Stdnet comes with two functions for
registration, a low level and a higher level one which can be used to register
several models at once.

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


Queries
==============================   

Query
~~~~~~~~~~~~~~~

.. autoclass:: Query
   :members:
   :member-order: bysource
   
   .. automethod:: __init__


.. _signal-api:

Signals
=====================
Stdnet includes a signal dispatcher which helps allow decoupled
applications get notified when actions occur elsewhere in the framework.
In a nutshell, signals allow certain senders to notify a set of receivers
that some action has taken place.
They’re especially useful when many pieces of code may be interested in
the same events.