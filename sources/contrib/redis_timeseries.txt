.. _redis-timeseries:

=============================
Redis timeseries API
=============================

**An implementation of a timeseries data structure in** redis_.

:Source: https://github.com/lsbardel/redis
:Python Client: http://lsbardel.github.com/python-stdnet/

Timeseries is an important data-structure not yet supported in redis,
it is represented by a unique sorted associative container,
that is to say it associates ordered unique times to values. 

Values, which can be anything you like, are ordered with respect to times (double values),
and can be accessed by times or rank (the order of times in the timeseries).
Times are unique, that is to say in a timeseries
there will be only one value associated with a time.

Internally, values are added to a hash table mapping them to times.
At the same time they are added to a skip list to maintain
sorting with respect to times.

Implementation is almost equivalent to zsets, and they look like zsets. But they are not zsets!

Performance::

	O(log(N)) on INSERT and REMOVE operations
	O(1) on RETRIEVAL via times

**Contents**

.. contents::
    :local:
    	
	
Commands
================
There are currently seven commands which cover the basic operations of a timeseries. I tried to keep them to a minimum
but there is scope to add more.
 
TSLEN
----------
Size of timeseries at ``key``, that is the number of ``rows`` in the timeseries::

    tslen key
 
TSADD
---------------
Add items to timeseries at ``key``::

	tsadd key time1 value1 time2 value2 ...
 
If value at ``time`` is already available, the value will be updated.
 

TSEXISTS
------------------
Check if ``time`` is in timeseries at ``key``::

    tsexists key time
 
TSGET
------
Get value at ``time`` in timeseries at ``key``::

    tsget key time
 
TSRANGE
------------------
Range by rank::

	tsrange key start end <flag>
 
Where ``start`` and ``end`` are integers following the same
Redis conventions as ``zrange``, ``<flag>`` is an optional
string which can take two values: ``withtimes`` or ``novalues``::

    tsrange key start end           -> return values
    tsrange key start end withtimes -> return (time,value)s
    tsrange key start end novalues  -> return times
 
TSRANGEBYTIME
------------------
Range by times::

    tsrangebytime time_start time_end <flag>
 
Where ``time_start`` and ``time_end`` are double (timestaps) and ``<flag>``
is the same as in ``TSRANGE``.

TSCOUNT
------------------
Count element in range by ``time``::

	tscount time_start,time_end
	
	
TSUNION
-----------------------------------------
**Not implemented**	
	
TSINTERCEPTION
-----------------------------------------
**Not implemented**


Use cases
================

* Store and manipulate financial timeseries::

    tsadd goog 
* A redis calendar.


Source code changes
==========================

I have tried, as much as possible, not to be intrusive so that it should be relatively straightforward to
track changes. In a nut shell, these are the additions/changes:

* Added 2 files in ``src``: t_ts.h_ and t_ts.c_.
* Modified redis.c_ to add timeseries commands to the command table and added the t_ts.h_ include.
* Modified Makefile_ so that t_ts.c_ is compiled.
* Modified object.c_ in ``decrRefCount`` and added t_ts.h_ include.
* Modified db.c_ in ``typeCommand`` and added t_ts.h_ include.
* Modified rdb.c_ in ``rdbSaveObject`` and ``rdbLoadObject`` and added t_ts.h_ include.
* Modified t_zset.c_ so that t_ts.c_ can use its internals. Check t_ts.h_ for details.
* Added 1 file in ``tests/unit/type``: ``ts.tcl``.

To run the timeseries tests::

    make test TAGS="ts"


.. _redis: http://redis.io/
.. _Makefile: https://github.com/lsbardel/redis/blob/timeseries/src/Makefile
.. _t_ts.c: https://github.com/lsbardel/redis/blob/timeseries/src/t_ts.c
.. _t_ts.h: https://github.com/lsbardel/redis/blob/timeseries/src/t_ts.h
.. _redis.c: https://github.com/lsbardel/redis/blob/timeseries/src/redis.c
.. _object.c: https://github.com/lsbardel/redis/blob/timeseries/src/object.c
.. _db.c: https://github.com/lsbardel/redis/blob/timeseries/src/db.c
.. _rdb.c: https://github.com/lsbardel/redis/blob/timeseries/src/rdb.c
.. _rdb.c: https://github.com/lsbardel/redis/blob/timeseries/src/rdb.c
.. _t_zset.c: https://github.com/lsbardel/redis/blob/timeseries/src/t_zset.c
