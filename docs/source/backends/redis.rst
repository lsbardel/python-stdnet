.. _redis-server:

==============
Redis
==============

Redis_ is an advanced key-value store where each key is associated with a value.
What makes Redis_ different from many other key-value databases, is that values can
be of different types:

    * Strings
    * Lists
    * Sets
    * Sorted Sets
    * Hash tables
    
In other words, you can look at redis as a data structure server, the networked
equivalent of the `standard template library in C++
<http://www2.roguewave.com/support/docs/hppdocs/stdref/index.html>`_.

Redis_ loads and maintains the whole data-set into memory, but the data-set
is persistent,
since at the same time it is saved on disk, so that when the server is restarted
data can be loaded back in memory. If you need speed, Redis_ is great solution.

.. note::

    ``stdnet`` is a made up word from ``std`` for Standard Template Library
    and ``net`` for networked.


Requirements
==================

* redis-py_, provides the standard redis client.
* pulsar_ optional. It is required by the :ref:`asynchronous connection <redis-async>`
  and the :ref:`publish/subscribe redis <redis_pubsub>` application.

.. _redis-connection-string:

Connection String
====================

The :ref:`connection string <connection-string>` is a way to specify
the various parameters of the backend to use. Redis supports the following
parameters:

* ``db``, the database number.
* ``namespace``, the namespace for all the keys used by the backend.
* ``password``, database password.
* ``timeout``, connection timeout (0 is an asynchronous connection).

A full connection string could be::

    redis://127.0.0.1:6379?db=3&password=bla&namespace=test.&timeout=5


Model data
==================
Each :class:`stdnet.odm.StdModel` class has an associated ``base key`` which
specifies the namespace for all keys associated with it::

    >>> from stdnet import getdb
    >>> from stdnet.apps.searchengine import WordItem
    >>> rdb = getdb('redis://localhost:6379?db=7&namespace=bla.')
    >>> rdb.basekey(WordItem._meta)
    'bla.searchengine.worditem'
     
Instances
~~~~~~~~~~~~~~~

Each :class:`stdnet.odm.StdModel` instance is mapped into a redis **Hash table**.
The hash table key is uniquely evaluated by the model hash and
the *id* of the model instance and it is stored at::

    <<basekey>>:obj:<<id>>
    
For example, a ``WordItem`` with id ``1`` is mapped by the database handler
in the code snipped above, into a redis hash table
at key ``bla.searchengine.worditem:obj:1``.
The hash fields and values are given by the field name and values of the
model instance.


Indexes
~~~~~~~~~~~~~~~~~

Indexes are obtained by using sets with keys obtained using the
following form::

    <<basekey>>:idx:<<field name>>:<<field value>>

If the model specify an :ref:`implicit ordering <implicit-sorting>` via the
:attr:`stdnet.odm.Metaclass.ordering` attribute, indexes are stored
in sorted sets rather than sets.


Unique Constratins
~~~~~~~~~~~~~~~~~~~~~~~~~

For some models you may need to specify certain field to be unique across
the Model. For example the following ``User`` model::

    class User(odm.StdModel):
        username = odm.SymbolField(unique=True)
        emauil = odm.SymbolField(unique=True)
        password = odm.CharField(required=True)

specifies two constrains.
In redis these constraints are stored into two separate hash tables with
key given by::

    <<basekey>>:uni:<<field name>>
    
Therefore our ``User`` model will have two additional hash tables at::

    <<basekey>>:uni:username
    <<basekey>>:uni:email
    
Each hash table map a field value to the ``id`` containing that value

.. _redis-parser:



Redis Session
===============================

Redis :class:`stdnet.odm.Session` and :class:`Query` are handled by lua scripts which
perform them in a single atomic operation.

Redis Query
=====================

A :class:`stdnet.odm.Query` is handled in redis by two different lua scripts:

* the first is script performs the aggregation of which results in a temporary
  redis ``key`` holding the ``ids`` resulting from the query operations.
* The second script is used to load the data from redis into the client.

.. _redis-aggragation:

Aggregation
~~~~~~~~~~~~~~~~~


Loading
~~~~~~~~~~~~~~~~~

The list of arguments passed to the :mod:`stdnet.lib.lua.load_query` script:

* ``query_key``, the redis key holding the ``ids``
  from the :ref:`aggregation step<redis-aggragation>`.
* ``basekey`` the prefix to apply to all keys in the model to aggregate.
* List of field to loads as ``[num_fields, field1, ...]``. if ``num_fields``
  is ``0``, all model fields will load.
* List of related model to load as ``[num_rel_models, rel_models1, ...]``.


.. _redis-async:

Asynchronous Connection
===========================

.. automodule:: stdnet.backends.redisb.async  
   

Client Extensions
=====================

.. automodule:: stdnet.backends.redisb.client


.. _Redis: http://redis.io/
.. _stdnet-redis: https://github.com/lsbardel/redis
.. _cython: http://cython.org/
.. _hiredis: https://github.com/antirez/hiredis
.. _redis-py: https://github.com/andymccurdy/redis-py
.. _pulsar: https://pypi.python.org/pypi/pulsar