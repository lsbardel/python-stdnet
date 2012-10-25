.. _db-index:

.. module:: stdnet

================================
Backend Data Server
================================

Behind the scenes is the database. Currently stdnet has support for Redis_ only but
an open door is left for including other backends.

Backends
===========

.. toctree::
   :maxdepth: 1
   
   redis
   

API
===========

Backend data server
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: BackendDataServer
   :members:
   :member-order: bysource
   
   
Backend Query
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: BackendQuery
   :members:
   :member-order: bysource


.. _Redis: http://redis.io/
.. _JSON: http://www.json.org/