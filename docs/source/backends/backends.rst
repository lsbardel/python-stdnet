.. _base-backend:

============================
Redis Database backend
============================

Redis_ is an advanced key-value store where each key is associated with a value.
What makes Redis different from many other key-value databases, is that values can
be of different types:

	* Strings
	* Lists
	* Sets
	* Sorted Sets
	* Hash tables
	
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
===============

Each :class:`stdnet.orm.StdModel` class is mapped to a redis **Hash table**.
The hash table key is uniquely evaluated by the model hash.
The hash fields are the model instances ids while values are JSON_
representation of the :ref:`scalar fields <atomfields>` and
:ref:`atom fields <objectfields>`.
Therefore, retrieving a model instance from its `id` is a O(1) operation.


Indexes
==========


Ordered Fields
==================
When a model contains ordered fields
(fields with the :attr:`stdnet.orm.Field.ordered` set to ``True``),
stdnet create as many extra hash tables as the number of ordered fields.
The hash tables have keys given by instances ids and values given by the
field value. This allowed the construction of the sorting algorithm::

    SORT idset BY 


Structured Fields
====================


API
=========

.. autoclass:: stdnet.BackendDataServer
   :members:
   :member-order: bysource


.. _Redis: http://redis.io/
.. _JSON: http://www.json.org/
