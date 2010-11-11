from stdnet import orm

from stdnet.contrib.timeserie import models


class DateTimeSeries(models.DateTimeSeries):
    ticker = orm.SymbolField(unique = True)
    
class TimeSeries(models.TimeSeries):
    ticker = orm.SymbolField(unique = True)

