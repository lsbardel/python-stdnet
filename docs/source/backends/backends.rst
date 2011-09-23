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

Each :class:`stdnet.orm.StdModel` instance is mapped to a redis **Hash table**.
The hash table key is uniquely evaluated by the model hash and
the *id* of the model instance.

The hash fields and values are given by the field name and values of the
model instance.


Indexes
~~~~~~~~~~~~~~~~~

Indexes are obtained by using sets or sorted sets.



.. _redis-parser:

Parser
~~~~~~~~~~~~~

Stdnet is shipped with a redis parser written in python. In addition it
supports hiredis-py_, a python extension module that wraps the priotocol
parsing code in hiredis_.

To install hiredis-py::

    pip install hiredis

If pyredis is installed it will be the default parser unless you override the
:ref:`settings.REDIS_PARSER <settings>` value and set it to ``python`` (you
would want to do that mainly for benchmarking reasons).


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
.. _hiredis-py: https://github.com/pietern/hiredis-py
.. _hiredis: https://github.com/antirez/hiredis
