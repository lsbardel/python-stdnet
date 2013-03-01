.. _tutorial-structures:


=======================================
Structures and Structure-Fields
=======================================

The :ref:`structure models <model-structures>` are the networked equivalent
of data-structures such as sets, hash-tables, lists and so forth.
They are one of the two :class:`Model` types supported in :mod:`stdnet`,
with the other covered in the :ref:`using models tutorial <tutorial>`.

The best way to manipulate structures is by using the
:ref:`session api <model-session>`::

    from stdnet import odm
    
    # get a session with redis backend
    session = odm.Session('redis://localhost:8888?db=3&namespace=test.')
    
    # Enter a transactional state
    with session.begin() as t:
        # create a new list structure
        list = t.add(odm.List('list1'))
        # add some data
        list.push_back(3)
        list.push_back('Hello')


These structures can also be used as :class:`stdnet.odm.StructureField` in
:class:`stdnet.odm.StdModel`.

.. _tutorial-list:

List
==============================

**Backends**: :ref:`redis <redis-server>`, :ref:`mongodb <mongo-server>`
  


.. _tutorial-timeseries:

Timiseries
==============================

**Backends**: :ref:`redis <redis-server>`

A timeseries is an ordered associative container where entries are ordered with
respect times and each entry is associated with a time. There are two
types of timeseries in stdnet: the :class:`stdnet.odm.TS` which accepts any
type of entry and the :class:`stdnet.apps.columnts.ColumnTS`, a specialized
:class:`stdnet.odm.TS` for multivariate numeric timeseries.

The :class:`stdnet.odm.TS` has a simple api::

    from datetime import date
    from stdnet import odm
    
    session = ...
    with session.begin() as t:
        ts = t.add(odm.TS())
        ts[date(2013,1,1)] = "New year's day!"