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
there are over 500 tests with a coverage of about 90%.

**The library is stable, used in production and continuously maintained**.

**Dependencies**: None

**Optional Dependencies**: cython_ for the :ref:`Redis C parser <redis-parser>`.


First steps
===============

**From scratch:**
:ref:`Overview and Installation <intro-overview>`

**Tutorials:**
:ref:`Using Models <tutorial>` |
:ref:`Query your data <tutorial-query>` |
:ref:`Sorting <sorting>` |
:ref:`Searching <tutorial-search>` |
:ref:`Twitter Clone<twitter-example>`

**API:**
:ref:`Structures API <model-structures>` |
:ref:`Model API <model-model>` |
:ref:`Fields API <model-field>` |
:ref:`Redis Client <redis-server>`

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
   examples/index
   model/index
   backends/index
   apps/index
   utility
   stdnetredis
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`

.. _Redis: http://redis.io/
.. _redis-py: https://github.com/andymccurdy/redis-py
.. _cython: http://cython.org/

