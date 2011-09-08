.. _stdnet-doc:

======================
Python Stdnet
======================

.. rubric:: An object relational mapper library for remote data-structures.
  Design to work with Redis_ data-store, but implemented so that
  other storage systems can be supported in the future.
  Simple to use and configure.

It includes a modified version of redis-py_ compatible with ``python 3``.
There are no dependencies
and the library requires ``python 2.6`` or above, including ``python 3``.
There are over 250 tests with a coverage of about 75%.

The library is stable, used in production and continuously maintained.



First steps
===============

**From scratch:**
:ref:`Overview and Installation <intro-overview>` |
:ref:`Tutorial <tutorial>`

**The API:**
:ref:`Model API <model-model>` |
:ref:`Fields API <model-field>`

**Examples:**
:ref:`Timeseries <timeseries-example>` |
:ref:`Twitter Clone<twitter-example>`

.. _contents:

Contents
===========

.. toctree::
   :maxdepth: 1
   
   overview
   model/index
   backends/index
   contrib/index
   examples/index
   changelog
   contributing
   utility

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Redis: http://code.google.com/p/redis/
.. _redis-py: https://github.com/andymccurdy/redis-py

