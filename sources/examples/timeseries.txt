.. _timeseries-example:

======================================
Time Series
======================================

A very simple example on how to manage timeseries_ data.
It requires the :ref:`stdnet redis branch <stdnetredis>` which
add :ref:`timeseries commands <redis-timeseries>` to vanilla redis.


Define the model
=====================
A simple model derived from :class:`stdnet.contrib.timeseries.models.TimeSeries` ::

    from datetime import date
    
    from stdnet import odm
    from stdnet.contrib.timeseries.models import TimeSeries
        
    
    class FinanceTimeSeries(TimeSeries):
        ticker = odm.SymbolField(unique = True)
        
        def __unicode__(self):
            return '%s - %s' % (self.ticker,self.data.size())


Register the model
=====================
Register ``FinanceTimeSeries`` to the standard backend::

    >>> from stdnet import odm
    >>> odm.register(FinanceTimeSeries)
    'redis db 7 on 127.0.0.1:6379'
    
    
Create Objects
===========================
    
    >>> from datetime import date
    >>> ts = FinanceTimeSeries(ticker = 'GOOG').save()
    >>> ts.data.add(date(2010,7,6),436.07)
    >>> ts.save()
    FinanceTimeSeries:: GOOG - 1
    >>> ts.data.add(date(2010,7,2),436.55)
    >>> ts.data.add(date(2010,7,1),439.49)
    >>> ts.save()
    FinanceTimeSeries:: GOOG - 3
    >>> for data in ts.data.items():
    ...     print data
    (datetime.date(2010,7,1), 439.49)
    (datetime.date(2010,7,2), 436.55)
    (datetime.date(2010,7,6), 436.07)
    
    
.. _timeseries: http://en.wikipedia.org/wiki/Time_series
