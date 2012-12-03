.. _db-index:

.. module:: stdnet

================================
Backend Data Server
================================

Behind the scenes we have the database.
Currently stdnet supports Redis_ and mongoDB_.

Backends
===========

.. toctree::
   :maxdepth: 1
   
   backends/redis
   backends/mongo
   

A backend instance is usually obtained via the :func:`getdb` function by
passing a valid connection string::

    getdb('redis://127.0.0.1:9739?db=7&namespace=test.')
    getdb('mongo://127.0.0.1:9739?db=test)
    
    

API
===========

getdb
~~~~~~~~~~~~~~~~

.. autofunction:: getdb


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
.. _mongoDB: http://docs.mongodb.org/
.. _JSON: http://www.json.org/