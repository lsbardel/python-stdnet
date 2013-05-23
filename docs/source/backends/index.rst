.. _db-index:

.. module:: stdnet

================================
Backend Servers
================================

Behind the scenes we have the database.
Currently stdnet supports Redis_ and mongoDB_.

Backends
===========

.. toctree::
   :maxdepth: 1
   
   api
   redis
   mongo
   
.. _connection-string:

A backend instance is usually obtained via the :func:`getdb` function by
passing a valid connection string::

    from pulsar import getdb
    
    b1 = getdb('redis://127.0.0.1:9739?db=7&namespace=test.')
    b2 = getdb('mongo://127.0.0.1:9739?db=test)
    
Check :ref:`redis connection strings <redis-connection-string>` for a
full list of valid parameters.

.. _Redis: http://redis.io/
.. _mongoDB: http://docs.mongodb.org/