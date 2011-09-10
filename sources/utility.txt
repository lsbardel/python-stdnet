.. _utility-index:

============================
Internals and Utilities
============================


.. module:: stdnet.orm

Model Utilities
============================

Instance form UUID
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.from_uuid


Model Iterator
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.model_iterator


Flush Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.flush_models



.. module:: stdnet.utils

.. _serialize-models:

Serialization
======================

Stdnet comes with a bunch of extendible utilities for serializing model data into
different formats.

Get serializer
~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.get_serializer


Register serializer
~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.register_serializer


Serializer
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.Serializer
   :members:
   :member-order: bysource
   
   
JSON utilities
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.utils.jsontools.JSONDateDecimalEncoder
   :members:
   :member-order: bysource


.. autoclass:: stdnet.utils.jsontools.date_decimal_hook
   :members:
   :member-order: bysource
      

Miscellaneous
============================

Populate
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.utils.populate



Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.PipeLine
   :members:
   :member-order: bysource


Exceptions
============================

.. autoclass:: stdnet.StdNetException
   :members:
   :member-order: bysource
   
.. autoclass:: stdnet.ImproperlyConfigured
   :members:
   :member-order: bysource
   
.. autoclass:: stdnet.ModelNotRegistered
   :members:
   :member-order: bysource
   
   
.. autoclass:: stdnet.QuerySetError
   :members:
   :member-order: bysource
   
.. autoclass:: stdnet.FieldError
   :members:
   :member-order: bysource
   
.. autoclass:: stdnet.FieldValueError
   :members:
   :member-order: bysource
   
