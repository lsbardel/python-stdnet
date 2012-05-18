'''\
An application for managing multivariate timeseries_. It provides
tools for performing aggregation and statistics via lua scripts.

The redis implementation uses several redis structures for a given
class:`ColumnTS` instance.

* A zset for holding times in an ordered fashion.
* A redis *set* for holding *fields* names.
* A redis string for each *field* in the timeseries.

This composite data-structure looks and feels like a zset.
However, the ordered set doesn't actually store the data, it is there to
maintain order and facilitate retrieval by times (scores) and rank.
  
For a given *field*, the data is stored in a sequence of 9-bytes string
with the initial byte (``byte0``) indicating the type of data::

    
    <byte0><byte1,...,byte8>
    
The API is straightforward::

    from datetime date
    from stdnet.apps.columnts ColumnTS
    
    ts = ColumnTS(id='test')
    ts.add(date(2012,2,21), {'open': 603.87, 'close': 614.00})
    
Analysis
=============
To perform analysis you write lua scripts::

    self:range()
    
    ts.evaluate(script)
    
API
======

.. autoclass:: ColumnTS
   :members:
   :member-order: bysource
   
.. _timeseries: http://en.wikipedia.org/wiki/Time_series
'''
from . import redis
from .encoders import *
from .models import *