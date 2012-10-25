.. _stdnet-doc:

======================
Python Stdnet
======================

.. rubric:: A networked data structures manager design to work with Redis_
  data-store, but implemented so that other storage systems can be supported
  in the future.
  
  
It is shipped with an object-data mapper which greatly facilitates the
management and retrieval of large data-sets.
It includes a stand-alone, ``python 3`` compatible,
:ref:`redis client <redis-client>` which was originally forked from redis-py_.
There are no dependencies, it requires ``python 2.6`` up to ``python 3.3`` and
there are over 600 tests with a coverage over 90%.

**The library is stable, used in production and continuously maintained**.

**Dependencies**: None

.. _contents:

Contents
===========

.. toctree::
   :maxdepth: 1
   
   overview
   examples/index
   api/index
   apps/index
   changelog
   stdnetredis
   

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`



.. _Redis: http://redis.io/
.. _redis-py: https://github.com/andymccurdy/redis-py
.. _cython: http://cython.org/

