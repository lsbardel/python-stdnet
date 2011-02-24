.. _base-backend:

============================
Database backend interface
============================

.. autoclass:: stdnet.BackendDataServer
   :members:
   :member-order: bysource


.. _redis-backend:

Redis backend
============================

Redis__ is an advanced key-value store where each key is associated with a value.
What makes Redis different from many other key-value databases, is that values can
be of different types:

	* Strings
	* Lists
	* Sets
	* Sorted Sets
	* Hash tables
	
In other words, you can look at redis as a data structures server,
the networked equivalent of the
`standard template library in C++ <http://www2.roguewave.com/support/docs/hppdocs/stdref/index.html>`_

Redis loads and mantains the whole dataset into memory, but the dataset is persistent,
since at the same time it is saved on disk, so that when the server is restarted
data can be loaded back in memory.

If you need speed, Redis is by far the best solution.

__ http://code.google.com/p/redis/
