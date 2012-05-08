'''\
An application for managing multivariate timeseries_. It provides
tools for performing aggregation and statistics via lua scripts.

The redis implementation uses several redis structures for a given
class:`ColumnTS` instance:

* A zset for holding times in an ordered fashion.
* A redis *set* for holding *fields* names.
* A redis string for each *field* in the timeseries.

The data for a given *field string* is stored in a sequence of 9-bytes strings
with the initial byte (``byte0``) indicating the type of data::

    
    <byte0><byte1,...,byte8>
    
The API is straightforward::

    from datetime date
    from stdnet.apps.columnts ColumnTS
    
    ts = ColumnTS(id='test')
    ts.add(date(2012,2,21), {'open': 603.87, 'close': 614.00})
    
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