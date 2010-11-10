.. _timeseries-example:

======================================
Time Series
======================================

A very simple example on how to manage timeseries data.

Define the model::

	from stdnet import orm
	from stdnet.utils import date2timestamp, timestamp2date
	
	class DateConverter(object):
	    @classmethod
	    def tokey(cls, value):
	        return date2timestamp(value)
	    
	    @classmethod
	    def tovalue(cls, value):
	        return timestamp2date(value).date()
        
	class TimeSerie(orm.StdModel):
	    ticker = orm.AtomField(unique = True)
	    data   = orm.HashField(converter = DateConverter)

``DateConverter`` is a module/object/class used to convert the keys to suitable
values which can be used as keys for the hash table. In this example the conversion from a 
Python ``datetime.date`` object to a JavaScript timestamp is performed.
        
Register ``TimeSerie`` to a backend::

    orm.register(TimeSerie,'redis://')
    

and create objects::
	
	>>> from datetime import date
	>>> ts = TimeSerie(ticker = 'GOOG').save()
	>>> ts.data.add(date(2010,7,6),436.07)
	>>> ts.save()
	TimeSerie: GOOG: 1
	>>> ts.data.add(date(2010,7,2),436.55)
	>>> ts.data.add(date(2010,7,1),439.49)
	>>> ts.save()
	TimeSerie: GOOG: 3
	>>> for data in ts.data.sorteditems():
	...     print data
	(datetime.date(2010,7,1), 439.49)
	(datetime.date(2010,7,2), 436.55)
	(datetime.date(2010,7,6), 436.07)
	>>> _
    
    
