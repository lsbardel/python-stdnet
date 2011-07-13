from stdnet import orm
from stdnet.utils import encoders

from stdnet.contrib.timeseries import models


class HashTimeSeries(models.HashTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class DateHashTimeSeries(models.DateHashTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class TimeSeries(models.TimeSeries):
    ticker = orm.SymbolField(unique = True)
    

class DateTimeSeries(models.DateTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class BigTimeSeries(models.DateTimeSeries):
    ticker = orm.SymbolField(unique = True)
    data  = models.TimeSeriesField(pickler = encoders.PythonPickle())
    

