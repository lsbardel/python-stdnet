.. _stdnetredis:

=======================
Stdnet Redis branch
=======================

During the development of stdnet_ we came across several design decisions, some
more critical than others, and most of them were resolved coding the client
only. However, more exotic features such as an efficient full text search and
time series required hacking on the server side.

For this reason we have the stdnet-redis_ branch.


Will stdnet-redis_ branch merged into redis_ master? This is a question for redis
core developers.


Sorted sets
==================================

For some bizzare reasons redis does not have a ``zdiffstore`` command.
For 99% of applications this is not an issue, but when using stdnet_ with
models which have an :ref:`implicit sorting <implicit-sorting>` it becomes one.
Sorted sets are like sets, a collection of elements with no repetion, but
unlike sets they are ordered with respect a score.

Sorted sets allow stdnet to implement models with an implicit sorting
with respect to a model field.


ZDIFFSTORE
------------------

Computes the difference between the first sorted set and all the successive sorted sets
given by the specified keys, and stores the result in destination.
It is mandatory to provide the number of input keys (numkeys)
before passing the input keys and the other (optional) arguments::

    ZDIFFSTORE destination numkeys key [key ...] [withscore]
    
If the optional argument ``withscore`` is ``True`` (default is ``False``), elements are
removed from the first sorted sets only if the score is matched.


.. _redis-timeseries:

Timeseries
==========================

Timeseries is an important data-structure not yet supported in redis,
it is represented by a unique sorted associative container,
that is to say it associates ordered unique times to values.

Values, which can be anything you like, are ordered with respect to double values (times),
and can be accessed by times or rank (the order of times in the timeseries).
Times are unique so that there will be only one value associated with a given time.

Internally, values are added to a skip list which maintain
sorting with respect to times.

Performance::

    O(log(N)) on INSERT REMOVE and RETRIEVAL via times


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
    
    
TSRANK
------------------
Returns the rank (index position) of ``time`` in timeseries at ``key``::

    tsrank key time
    
 
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

    tsrangebytime key time_start time_end <flag>
 
Where ``time_start`` and ``time_end`` are double (timestaps) and ``<flag>``
is the same as in ``TSRANGE``.


TSCOUNT
------------------
Count element in range by ``time``::

    tscount key time_start,time_end
    
This command is similar to :ref:`ZCOUNT http://redis.io/commands/zcount` for
sorted sets.

    
    
Sorting
==================

The stdnet branch includes a slightly modified sort_ command which can accept
an extra parameter called ``storeset``.

This is similar to the ``store`` parameter with the only
difference that the result of the sorting algorithm will be stored
in a set rather than in a list.

At this point, the interested reader may spot a conundrum here. Why would you want
to store the result of a sort algorithm into a data structure which 
does not maintain ordering?

The usage of this parameter in conjunction with the ``BY nosort`` inputs
in the following patter::

    SORT mydata BY nosort GET object_* STORESET resultkey
    
or::

    SORT mydata BY nosort GET *_field STORESET resultkey
    
Essentially not performing any sorting whatsoever, instead the command
aggregated fields of hashtable/s into a set.


Source code changes
==========================

* Added 2 files in ``src``: ``t_ts.h`` and ``t_ts.c``.
* Modified ``redis.c`` to add extra commands to the command table and added the ``t_ts.h`` include.
* Modified ``Makefile`` so that ``t_ts.c`` is compiled.
* Modified ``object.c`` in ``decrRefCount`` and added ``t_ts.h`` include.
* Modified ``db.c`` in ``typeCommand`` and added ``t_ts.h`` include.
* Modified ``rdb.c`` in ``rdbSaveObject`` and ``rdbLoadObject`` and added ``t_ts.h`` include.


t_zset.c
----------
* Modified so that ``t_ts.c`` can use its internals.
* Modified ``zunionInterGenericCommand`` function to accommodate the ZDIFFSTORE command.

sort.c
--------
* Modified the ``sortCommand`` to accomodate for the ``storeset`` parameter.

Tests
-------
Added 1 file in ``tests/unit/type``: ``ts.tcl``.

To run the timeseries tests::

    make test TAGS="ts"


.. _redis: http://redis.io/
.. _sort:  http://redis.io/commands/sort
.. _stdnet-redis: https://github.com/lsbardel/redis
.. _stdnet: http://lsbardel.github.com/python-stdnet/