from stdnet import orm
from stdnet.contrib.timeserie import models

    

class TimeSerie(models.TimeSerie):
    data    = models.TimeSerieField(converter = models.DateConverter)
    ticker  = orm.AtomField(unique = True)
    
    def __str__(self):
        return '%s: %s' % (self.ticker,self.data.size())