.. _tutorial-structures:


=======================================
Structures and Structure-Fields
=======================================

.. _tutorial-list:

List
==============================

**Available**: :ref:`redis 


.. _tutorial-timeseries:

Timiseries
==============================

A timeseries is an ordered associative container where entries are ordered with
respect times and each entry is associated with a time. There are two
types of timeseries in stdnet: the :class:`stdnet.Timeseries` which accept any
type of entry and the :class:`stdnet.apps.columnts.ColumnTS`, a specialized
:class:`stdnet.Timeseries` for multivariate numeric timeseries.