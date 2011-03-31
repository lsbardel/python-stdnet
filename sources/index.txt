.. _stdnet-doc:

======================
Python StdNet
======================

.. rubric:: An object relational mapper library for remote data-structures.
  Design to work with Redis_ data-store, but implemented so that
  other storage systems can be supported in the future.

Create a model, register it to a back-end
data structure server and create objects.
Simple to use and configure.

It includes a modified version of redis-py_ compatible with ``python 3``. There are no dependencies
and the library requires ``python 2.6`` or above, including ``python 3``.
There are over 180 tests with a coverage of about 70%.

The library is stable and used in production.


First steps
===============

**From scratch:**
:ref:`Overview and Installation <intro-overview>`

**Models and QuerySets:**
:ref:`Executing queries <model-query>` | :ref:`Model API <model-model>`  

**Examples:**
:ref:`Timeseries <timeseries-example>` | :ref:`Portfolio <portfolio-example>` |
:ref:`Twitter Clone<twitter-example>`

.. _contents:

Contents
===========

.. toctree::
   :maxdepth: 1
   
   model/index
   backends/index
   utility/index
   contrib/index
   examples/index
   changelog
   contributing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Redis: http://code.google.com/p/redis/
.. _redis-py: https://github.com/andymccurdy/redis-py

