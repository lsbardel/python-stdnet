.. _tutorial-structures:


=======================================
Structure-Fields
=======================================

The :ref:`structure fields <model-field-structure>` are the networked equivalent
of data-structures such as sets, hash-tables, lists and so forth. They
are associated with an instance of a :class:`StdModel` or, for redis
backend only, with a :class:`StdModel` class.

.. module:: stdnet.odm

.. _tutorial-list:

List
==============================

**Backends**: :ref:`redis <redis-server>`, :ref:`mongodb <mongo-server>`

**default encoder**: :class:`stdnet.utils.encoders.NumericDefault`.

**Methods**

The following api methods are available for a :class:`ListField` via the
:class:`List` structure:

* :meth:`Structure.size` list size.
* :meth:`Structure.items` retrieve all items for a list.
* :meth:`Sequence.push_back` append an element at the end of the list.
* :meth:`List.push_front` prepend an element at the beginning of the list.
* :meth:`Sequence.pop_back` remove the last element the list.
* :meth:`List.pop_front` remove the first element of the list.

.. _tutorial-set:

Set
==============================

**Backends**: :ref:`redis <redis-server>`, :ref:`mongodb <mongo-server>`

**default encoder**: :class:`stdnet.utils.encoders.NumericDefault`.

**Methods**

The following api methods are available for a :class:`Set` and :class:`SetField`:

* :meth:`Structure.size` set size.
* :meth:`Set.add` add a new element to the set.
* :meth:`Set.update` add a collection of elements to the set.
* :meth:`Set.discard` remove an element from the set if it is a member.
* :meth:`Set.remove` remove an element from the set. Raises an :class:`KeyError`
  if not available.
* :meth:`Set.difference_update` remove a collection of elements from the set.


.. _tutorial-zset:

Zset
==============================

**Backends**: :ref:`redis <redis-server>`

**default encoder**: :class:`stdnet.utils.encoders.NumericDefault`.

**Methods**

The following api methods are available for a :class:`Zset` and
:class:`SetField` with :class:`SetField.ordered` attribute set to ``True``:

* :meth:`Structure.size` set size.
* :meth:`Set.add` add a new element to the set.
* :meth:`Zset.rank` the rank (position) of an element withing the ordered set.
* :meth:`Set.update` add a collection of elements to the set.
* :meth:`Set.discard` remove an element from the set if it is a member.
* :meth:`Set.remove` remove an element from the set. Raises an :class:`KeyError`
  if not available.
* :meth:`Set.difference_update` remove a collection of elements from the set.
* :meth:`OrderedMixin.range` the specified range of elements in the sorted set.
* :meth:`OrderedMixin.irange` the specified range of elements by index.
* :meth:`OrderedMixin.pop_range` remove the specified range of elements in
  the sorted set.
* :meth:`OrderedMixin.ipop_range` remove the specified range of elements
  by index.


.. _tutorial-hash:

Hash table
==============================

**Backends**: :ref:`redis <redis-server>`

**default field encoder**: :class:`stdnet.utils.encoders.Default`.

**default value encoder**: :class:`stdnet.utils.encoders.NumericDefault`.

**Methods**

The following api methods are available for a :class:`HashTable` and :class:`HashField`:

* :meth:`Structure.size` Hash table size.
* :meth:`Set.add` add a new element to the set.
* :meth:`Set.update` add a collection of elements to the set.
* :meth:`Set.discard` remove an element from the set if it is a memeber.
* :meth:`Set.remove` remove an element from the set.
* :meth:`Set.difference_update` remove a collection of elements from the set.



.. _tutorial-timeseries:

Timiseries
==============================

**Backends**: :ref:`redis <redis-server>`

**default field encoder**: :class:`stdnet.utils.encoders.DateTimeConverter`.

**default value encoder**: :class:`stdnet.utils.encoders.Json`.

A timeseries is an ordered associative container where entries are ordered with
respect times and each entry is associated with a time. There are two
types of timeseries in stdnet: the :class:`stdnet.odm.TS` which accepts any
type of entry and the :class:`stdnet.apps.columnts.ColumnTS`, a specialized
:class:`stdnet.odm.TS` for multivariate numeric timeseries.

The :class:`TS` has a simple api::

    from datetime import date
    from stdnet import odm
    
    session = ...
    with session.begin() as t:
        ts = t.add(odm.TS())
        ts[date(2013,1,1)] = "New year's day!"
        
**Methods**

The following api methods are available for a :class:`TS` and :class:`TimeSeriesField`:

* :meth:`Structure.size`
* :meth:`OrderedMixin.range` retrieve a range between two dates/datetimes.
* :meth:`OrderedMixin.irange` retrieve a range by rank.
* :meth:`OrderedMixin.pop_range` remove a range between two dates/datetimes.
* :meth:`OrderedMixin.ipop_range` remove a range by rank.