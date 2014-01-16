.. _stdnet-doc:

======================
Python Stdnet
======================

.. rubric:: Object data mapper and advanced query manager for redis.

.. _requirements:

Requirements
================

* redis-py_ for :ref:`redis backend <redis-server>`. Redis 2.6 or above.
* Optional cython_ for :ref:`ultra-fast C-extensions <c-extensions>`. Recommended.
* Optional pymongo_ for :ref:`mongo db backend <mongo-server>` (pre-alpha).
* Optional pulsar_ for :ref:`asynchronous database connection <tutorial-asynchronous>`.

.. _contents:

Contents
===========

.. toctree::
   :maxdepth: 1

   overview
   examples/index
   backends/index
   changelog
   faq
   api/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`


Legacy
============

.. toctree::
   :maxdepth: 1

   stdnetredis

.. _Redis: http://redis.io/
.. _redis-py: https://github.com/andymccurdy/redis-py
.. _cython: http://cython.org/
.. _Mongodb: http://www.mongodb.org/
.. _pymongo: http://api.mongodb.org/python/current/index.html
.. _pulsar: https://pypi.python.org/pypi/pulsar
