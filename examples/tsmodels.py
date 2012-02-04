from stdnet import orm
from stdnet.utils import encoders, todatetime, todate, missing_intervals

        
class TimeSeries(orm.StdModel):
    ticker = orm.SymbolField(unique = True)
    data  = orm.TimeSeriesField()
    
    def todate(self, v):
        return todatetime(v)
    
    def dates(self):
        return self.data
    
    def items(self):
        return self.data.items()
    
    def __get_start(self):
        r = self.data.front()
        if r:
            return r[0]
    data_start = property(__get_start)
    
    def __get_end(self):
        r = self.data.back()
        if r:
            return r[0]
    data_end = property(__get_end)
    
    def size(self):
        '''number of dates in timeseries'''
        return self.data.size()
    
    class Meta:
        abstract = True
        
    def intervals(self, startdate, enddate, parseinterval = None):
        '''Given a ``startdate`` and an ``enddate`` dates, evaluate the date intervals
from which data is not available. It return a list of two-dimensional tuples
containing start and end date for the interval. The list could countain 0,1 or 2
tuples.'''
        return missing_intervals(startdate, enddate, self.data_start,
                                 self.data_end, dateconverter = self.todate,
                                 parseinterval = parseinterval)
    

class DateTimeSeries(TimeSeries):
    data  = orm.TimeSeriesField(pickler = encoders.DateConverter())
    
    def todate(self, v):
        return todate(v)
    
    
class BigTimeSeries(DateTimeSeries):
    data  = orm.TimeSeriesField(
                    pickler = encoders.DateConverter(),
                    value_pickler = encoders.PythonPickle())
