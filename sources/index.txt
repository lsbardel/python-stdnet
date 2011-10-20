.. _stdnet-doc:

======================
Python Stdnet
======================

.. rubric:: A networked data structures manager design to to work with Redis_
  data-store, but implemented so that other storage systems can be supported
  in the future.
  
  
It is shipped with an object relational mapper which greatly facilitates the
management and retrieval of large data-sets.
It includes a modified version of redis-py_ compatible with ``python 3``.
There are no dependencies
and the library requires ``python 2.6`` or above, including ``python 3``.
There are over 300 tests with a coverage of about 80%.

**The library is stable, used in production and continuously maintained**.

**Dependencies**: None

**Optional Dependencies**: cython_ for the :ref:`Redis C parser <redis-parser>`.


First steps
===============

**From scratch:**
:ref:`Overview and Installation <intro-overview>`

**Tutorials:**
:ref:`Tutorial 1 <tutorial>` |
:ref:`Tutorial 2 <tutorial2>`

**API:**
:ref:`Structures <structures-backend>` |
:ref:`Model API <model-model>` |
:ref:`Fields API <model-field>`

**Examples:**
:ref:`Timeseries <timeseries-example>` |
:ref:`Twitter Clone<twitter-example>`

**Miscellaneous:**
:ref:`Contributing <contributing>` |
:ref:`Tests <runningtests>` |
:ref:`Changelog <changelog>` |
:ref:`Kudos <kudos>`

.. _contents:

Contents
===========

.. toctree::
   :maxdepth: 1
   
   overview
   model/index
   backends/index
   apps/index
   examples/index
   utility
   stdnetredis
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Redis: http://redis.io/
.. _redis-py: https://github.com/andymccurdy/redis-py
.. _cython: http://cython.org/

