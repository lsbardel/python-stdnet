'''\
An application which implements a specialised remote
:class:`stdnet.odm.Structure` for managing numeric multivariate
timeseries_ and perform remote analysis on them. The main classes
for this application are :class:`ColumnTS`, the stand alone data structure, and
the correspondent :class:`ColumnTSField` which can be used as
a :class:`stdnet.odm.StructureField` on a :class:`stdnet.odm.StdModel`.

    
The API is straightforward::

    from datetime import date
    from stdnet.apps.columnts import ColumnTS
    
    ts = ColumnTS(id='test')
    ts.add(date(2012,2,21), {'open': 603.87, 'close': 614.00})
    
It can also be used as a :ref:`datastructure fields <model-field-structure>`.
For example::

    from stdnet import odm
    from stdnet.apps.columnts import ColumnTSField
    
    class Ticker(odm.StdModel):
        code = odm.SymbolField()
        data = ColumnTSField()
        
    
Analysis
=============
To perform analysis you write lua scripts::

    self:range()
    
    ts.evaluate(script)
    
API
======

ColumnTS
~~~~~~~~~~~~~~

.. autoclass:: ColumnTS
   :members:
   :member-order: bysource
   

ColumnTSField
~~~~~~~~~~~~~~

.. autoclass:: ColumnTSField
   :members:
   :member-order: bysource


Redis Implementation
========================

It provides tools for performing aggregation and statistics
via lua scripts.
The redis implementation uses several redis structures for a given
:class:`ColumnTS` instance.

* A zset for holding times in an ordered fashion.
* A redis *set* for holding *fields* names.
* A redis string for each *field* in the timeseries for holding values.

This composite data-structure looks and feels like a redis zset.
However, the ordered set doesn't actually store the data, it is there to
maintain order and facilitate retrieval by times (scores) and rank.
  
For a given *field*, the data is stored in a sequence of 9-bytes string
with the initial byte (``byte0``) indicating the type of data::

    
    <byte0><byte1,...,byte8>
    
       
.. _timeseries: http://en.wikipedia.org/wiki/Time_series
'''
from . import redis
from .models import *