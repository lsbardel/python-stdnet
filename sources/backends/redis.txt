.. _redis-server:

==============
Redis Backend
==============

Redis_ is an advanced key-value store where each key is associated with a value.
What makes Redis different from many other key-value databases, is that values can
be of different types:

    * Strings
    * Lists
    * Sets
    * Sorted Sets
    * Hash tables
    * :ref:`Timeseries <redis-timeseries>` (only if you use the stdnet-redis_ branch)
    
In other words, you can look at redis as a data structure server, the networked
equivalent of the `standard template library in C++
<http://www2.roguewave.com/support/docs/hppdocs/stdref/index.html>`_.
And that is where stdnet get its name from, *std* from the standard template library
and *net* from networked.

Redis loads and mantains the whole dataset into memory, but the dataset is persistent,
since at the same time it is saved on disk, so that when the server is restarted
data can be loaded back in memory. If you need speed, Redis is great solution.


Model data
==================
Each :class:`stdnet.odm.StdModel` class has an associated ``base key`` which
specify the namespace for all keys associated with it::

    >>> from stdnet import getdb
    >>> from stdnet.apps.searchengine import Word
    >>> rdb = getdb('redis://localhost:6379?prefix=bla.')
    >>> rdb.basekey(Word._meta)
    'bla.searchengine.word'
     
Instances
~~~~~~~~~~~~~~~

Each :class:`stdnet.odm.StdModel` instance is mapped into a redis **Hash table**.
The hash table key is uniquely evaluated by the model hash and
the *id* of the model instance and it is stored at::

    <<basekey>>:obj:<<id>>
    

The hash fields and values are given by the field name and values of the
model instance.


Indexes
~~~~~~~~~~~~~~~~~

Indexes are obtained by using sets with keys obtained using the
following form::

    <<basekey>>:idx:<<field name>>:<<field value>>

If the model specify an :ref:`implicit ordering <implicit-sorting>` via the
:attr:`stdnet.odm.base.Metaclass.ordering` attribute, indexes are stored
in sorted sets rather than sets.


Indices are updated and removed via the ``update_indices`` function in the
``section.lua`` script.


Unique Constratins
~~~~~~~~~~~~~~~~~~~~~~~~~

For some models you may need to specify certain field to be unique across
the Model. For example the following ``User`` model::

    class User(odm.StdModel):
        username = odm.SymbolField(unique = True)
        emauil = odm.SymbolField(unique = True)
        password = odm.CharField(required = True)

specifies two constrains.
In redis these constraints are stored into two separate hash tables with
key given by::

    <<basekey>>:uni:<<field name>>
    
Therefore our ``User`` model will have two additional hash tables at::

    <<basekey>>:uni:username
    <<basekey>>:uni:email
    
Each hash table map a field value to the ``id`` containing that value

.. _redis-parser:

Parser
==============

Stdnet is shipped with a redis parser written in python and a faster version
written in C. In order to use the C parser you need to have installed
cython_.
The C parser wraps the protocol parsing code in hiredis_ and it is available
for both windows and linux. To just compile the extensions::

    python setup.py build_ext

If extensions are are installed dusring setup,
the C parser will be the default parser unless you set the
:ref:`settings.REDIS_PY_PARSER <settings>` value to ``True``
(you would want to do that mainly for benchmarking reasons).

To check if the extensions are available::

    >>> from stdnet import lib
    >>> lib.hasextensions
    True
    >>> _
        
In windows you may need to install mingw_ and install using the command::

    python setup.py build -c mingw32
    python setup.py install


.. _mingw: http://www.mingw.org/

.. _redis-client:

Redis client API
==================

.. automodule:: stdnet.lib.redis

Redis
~~~~~~~~~~~~~
.. autoclass:: Redis
   :members:
   :member-order: bysource
   

Pipeline
~~~~~~~~~~~~~~~
.. autoclass:: Pipeline
   :members:
   :member-order: bysource
   
   
Connection Pool
~~~~~~~~~~~~~~~
.. autoclass:: ConnectionPool
   :members:
   :member-order: bysource
   
   
Connection
~~~~~~~~~~~~~~~
.. autoclass:: Connection
   :members:
   :member-order: bysource
   


Redis Session
===============================

Redis :class:`stdnet.odm.Session` and :class:`Query` are handled by lua scripts which
perform them in a single atomic operation.

Redis Query
=====================

A :class:`stdnet.odm.Query` is handled by two different lua scripts, the first is script
perform the aggregation of which results in a temporary redis ``key``
holding the ``ids`` which result from the query operations.
The second script is used to load the data from redis to the client.

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
    
   
   
.. _Redis: http://redis.io/
.. _stdnet-redis: https://github.com/lsbardel/redis
.. _cython: http://cython.org/
.. _hiredis: https://github.com/antirez/hiredis