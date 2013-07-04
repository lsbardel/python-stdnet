.. _utility-index:

============================
Internals and Utilities
============================

.. _settings:

Default Settings
=========================

.. automodule:: stdnet.utils.conf


.. module:: stdnet.odm


.. _serialize-models:

Serialization
======================

Stdnet comes with a bunch of extendible utilities for
:ref:`serializing model <tutorial-serialise>` data into different formats.

Get serializer
~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: get_serializer


Register serializer
~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: register_serializer


Serializer
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Serializer
   :members:
   :member-order: bysource
   

JsonSerializer
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: JsonSerializer
   :members:
   :member-order: bysource
   

.. module:: stdnet.utils
   
JSON utilities
=====================

.. automodule:: stdnet.utils.jsontools

.. _encoders:

Encoders
=======================

.. automodule:: stdnet.utils.encoders


.. module:: stdnet

Exceptions
============================

.. autoclass:: StdNetException
   :members:
   :member-order: bysource
   
.. autoclass:: ImproperlyConfigured
   :members:
   :member-order: bysource   
   
.. autoclass:: QuerySetError
   :members:
   :member-order: bysource
   
.. autoclass:: FieldError
   :members:
   :member-order: bysource
   
.. autoclass:: FieldValueError
   :members:
   :member-order: bysource
   

.. _signal-api:

Signals
=====================
Stdnet includes a signal dispatcher which helps allow decoupled
applications get notified when actions occur elsewhere in the framework.
In a nutshell, signals allow certain senders to notify a set of receivers
that some action has taken place.
They are especially useful when many pieces of code may be interested in
the same events.

The data mapper provide with the following built-in signals in the :mod:`stdnet.odm`
module:

* ``pre_commit`` triggered before new instances or changes on existing instances
  are committed to the backend server.
* ``post_commit`` triggered after new instances or changes on existing instances
  are committed to the backend server.

It is also possible to add callback to single instances in the following way::

    instance = MyModel(...)
    instance.post_commit(callable)

   
Miscellaneous
============================

Populate
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.utils.populate


.. _api-testing:

Testing
======================

.. automodule:: stdnet.utils.test