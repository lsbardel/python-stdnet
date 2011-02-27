from stdnet import orm

from stdnet.contrib.timeseries import models


class HashTimeSeries(models.HashTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class DateHashTimeSeries(models.DateHashTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
    
class TimeSeries(models.TimeSeries):
    ticker = orm.SymbolField(unique = True)
    

