.. _utility-index:

============================
Internals and Utilities
============================

.. _settings:

Default Settings
=========================

.. automodule:: stdnet.conf


.. module:: stdnet.odm

Model Utilities
============================

Instance form UUID
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: from_uuid


Model Iterator
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: model_iterator


Flush Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.odm.flush_models


.. _serialize-models:

Serialization
======================

Stdnet comes with a bunch of extendible utilities for serializing model data into
different formats.

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
   

.. module:: stdnet.utils
   
JSON utilities
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: JSONDateDecimalEncoder
   :members:
   :member-order: bysource


.. autoclass:: date_decimal_hook
   :members:
   :member-order: bysource
      

.. _encoders:

.. module:: stdnet.utils.encoders

Encoders
=======================

.. autoclass:: Encoder
   :members:
   :member-order: bysource


No Encoder
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: NoEncoder
   :members:
   :member-order: bysource


Default
~~~~~~~~~~~~~~~~~~~~
   
.. autoclass:: Default
   :members:
   :member-order: bysource


Bytes
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Bytes
   :members:
   :member-order: bysource


Json
~~~~~~~~~~~~~~~~~~~~
   
.. autoclass:: Json
   :members:
   :member-order: bysource


Python Pickle
~~~~~~~~~~~~~~~~~~~~
   
.. autoclass:: PythonPickle
   :members:
   :member-order: bysource


DateTimeConverter
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DateTimeConverter
   :members:
   :member-order: bysource
   
   
DateConverter
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DateConverter
   :members:
   :member-order: bysource


.. module:: stdnet

Exceptions
============================

.. autoclass:: StdNetException
   :members:
   :member-order: bysource
   
.. autoclass:: ImproperlyConfigured
   :members:
   :member-order: bysource
   
.. autoclass:: ModelNotRegistered
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


.. module:: stdnet.test

TestCase
~~~~~~~~~~~~~~~

.. autoclass:: TestCase
   :members:
   :member-order: bysource
