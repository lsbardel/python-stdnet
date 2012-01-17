from stdnet import orm
from stdnet.utils import encoders
from stdnet.apps import timeseries     


class HashTimeSeries(timeseries.HashTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class DateHashTimeSeries(timeseries.DateHashTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class TimeSeries(timeseries.TimeSeries):
    ticker = orm.SymbolField(unique = True)
    

class DateTimeSeries(timeseries.DateTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class BigTimeSeries(timeseries.DateTimeSeries):
    ticker = orm.SymbolField(unique = True)
    data  = orm.TimeSeriesField(pickler = encoders.PythonPickle())