.. _db-index:

.. module:: stdnet

================================
Backend Servers
================================

Behind the scenes we have the database.
Currently stdnet supports Redis_.

Backends
===========

.. toctree::
   :maxdepth: 1

   api
   redis

.. _connection-string:

A backend instance is usually obtained via the :func:`getdb` function by
passing a valid connection string::

    from pulsar import getdb

    b1 = getdb('redis://127.0.0.1:9739?db=7&namespace=test.')

Check :ref:`redis connection strings <redis-connection-string>` for a
full list of valid parameters.

.. _Redis: http://redis.io/
