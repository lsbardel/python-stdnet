.. _redis-backend:

============================
Database Backends
============================

Behind the scenes is the database. Currently stdnet has support for Redis_ only but
an open door is left for including other backends.

Redis
==============

Redis_ is an advanced key-value store where each key is associated with a value.
What makes Redis different from many other key-value databases, is that values can
be of different types:

	* Strings
	* Lists
	* Sets
	* Sorted Sets
	* Hash tables
	* Timeseries if you use the redis branch from lsbardel_.
	
In other words, you can look at redis as a data structures server,
the networked equivalent of the
`standard template library in C++ <http://www2.roguewave.com/support/docs/hppdocs/stdref/index.html>`_.
And that is where stdnet get its name from,
*std* from the standard template library
and *net* from networked.

Redis loads and mantains the whole dataset into memory, but the dataset is persistent,
since at the same time it is saved on disk, so that when the server is restarted
data can be loaded back in memory. If you need speed, Redis is great solution.

Model data
~~~~~~~~~~~~~~~~~~
Each :class:`stdnet.orm.StdModel` class has an associated ``base key`` which
specify the namespace for all keys associated with it::

    >>> from stdnet import getdb
    >>> from stdnet.apps.searchengine import Word
    >>> rdb = getdb('redis://localhost:6379?prefix=bla.')
    >>> rdb.basekey(Word._meta)
    'bla.searchengine.word'
     
Each :class:`stdnet.orm.StdModel` instance is mapped to a redis **Hash table**.
The hash table key is uniquely evaluated by the model hash and
the *id* of the model instance.

The hash fields and values are given by the field name and values of the
model instance.


Indexes
~~~~~~~~~~~~~~~~~

Indexes are obtained by using sets or sorted sets with keys obtained using the
following form::

    <<basekey>>:idx:<<field name>>:<<field value>>

Indices are updated and removed via the ``update_indices`` function in the
``section.lua`` script.


.. _redis-parser:

Parser
~~~~~~~~~~~~~

Stdnet is shipped with a redis parser written in python and a faster version
written in C. In order to use the C parser you need to have installed
cython_.
The C parser wraps the priotocol parsing code in hiredis_ and it is available
for both windows and linux. To compile the exenstions::

    python setup.py build_ext

If extensions are is installed the C parser will be the default parser
unless you override the :ref:`settings.REDIS_PARSER <settings>` value
and set it to ``python`` (you would want to do that mainly
for benchmarking reasons).

To check if the extensions are available::

    >>> from stdnet import lib
    >>> lib.hasextensions
    True
    >>> _
    


API
===========

Backend data server
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.BackendDataServer
   :members:
   :member-order: bysource


.. _Redis: http://redis.io/
.. _JSON: http://www.json.org/
.. _lsbardel: https://github.com/lsbardel/redis
.. _cython: http://cython.org/
.. _hiredis: https://github.com/antirez/hiredis
