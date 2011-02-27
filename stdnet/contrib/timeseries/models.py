from stdnet import orm
from stdnet.utils import date2timestamp, timestamp2date, todatetime, todate

from .utils import default_parse_interval


class DateTimeConverter(object):
    
    @classmethod
    def tokey(cls, value):
        return date2timestamp(value)
    
    @classmethod
    def tovalue(cls, value):
        return timestamp2date(value)
    

class DateConverter(object):
    
    @classmethod
    def tokey(cls, value):
        return date2timestamp(value)
    
    @classmethod
    def tovalue(cls, value):
        return timestamp2date(value).date()


class HashTimeSeriesField(orm.HashField):
    '''To be used with subclasses of :class:`TimeSeriesBase`.'''
    def register_with_model(self, name, model):
        super(HashTimeSeriesField,self).register_with_model(name, model)
        self.converter = model.converter
        
        
class TimeSeriesField(orm.MultiField):
    '''A new timeseries field based on TS data structure in Redis.
To be used with subclasses of :class:`TimeSeriesBase`'''
    type = 'ts'
    
    def get_pipeline(self):
        return 'ts'
    
    def __init__(self, *args, **kwargs):
        super(TimeSeriesField,self).__init__(*args, **kwargs)
        
    def register_with_model(self, name, model):
        self.converter = model.converter # must be set before calling super method
        super(TimeSeriesField,self).register_with_model(name, model)
        

class TimeSeriesBase(orm.StdModel):
    '''Timeseries base model class'''
    converter = DateTimeConverter
    '''Class responsable for converting Python dates into unix timestamps'''
    
    def todate(self, v):
        return todatetime(v)
    
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
        parseinterval = parseinterval or default_parse_interval
        start     = self.start
        end       = self.end
        todate    = self.todate
        startdate = todate(parseinterval(startdate,0))
        enddate   = max(startdate,todate(parseinterval(enddate,0)))

        calc_intervals = []
        # we have some history already
        if start:
            # the startdate is already in the database
            if startdate < start:
                calc_start = startdate
                calc_end = parseinterval(start, -1)
                if calc_end >= calc_start:
                    calc_intervals.append((calc_start, calc_end))

            if enddate > end:
                calc_start = parseinterval(end, 1)
                calc_end = enddate
                if calc_end >= calc_start:
                    calc_intervals.append((calc_start, calc_end))
        else:
            start = startdate
            end = enddate
            calc_intervals.append((startdate, enddate))

        if calc_intervals:
            # There are calculation intervals, which means the
            # start and aned date have changed
            N = len(calc_intervals)
            start1 = calc_intervals[0][0]
            end1 = calc_intervals[N - 1][1]
            if start:
                start = min(start, start1)
                end = max(end, end1)
            else:
                start = start1
                end = end1

        return calc_intervals 
        
    
class TimeSeries(TimeSeriesBase):
    '''Timeseries model'''
    data  = TimeSeriesField()
    
    def dates(self):
        return self.data.keys()
    
    def items(self):
        return self.data.items()
    
    def __get_start(self):
        return self.data.front()
    start = property(__get_start)
    
    def __get_end(self):
        return self.data.back()
    end = property(__get_end)
        

class HashTimeSeries(TimeSeriesBase):
    '''Base abstract class for timeseries'''
    data  = HashTimeSeriesField()
    start = orm.DateTimeField(required = False, index = False)
    end   = orm.DateTimeField(required = False, index = False)
    
    def dates(self):
        return self.data.sortedkeys()
    
    def items(self):
        return self.data.sorteditems()
    
    def save(self):
        supersave = super(HashTimeSeries,self).save
        supersave()
        self.storestartend()
        return supersave()
    
    def storestartend(self):
        '''Store the start/end date of the timeseries'''
        dates = self.data.sortedkeys()
        if dates:
            self.start = dates[0]
            self.end   = dates[-1]
        else:
            self.start = None
            self.end   = None
    
    def fromto(self):
        if self.start:
            return '%s - %s' % (self.start.strftime('%Y %m %d'),self.end.strftime('%Y %m %d'))
        else:
            return ''
        
    def __str__(self):
        return self.fromto()


class DateHashTimeSeries(HashTimeSeries):
    converter = DateConverter
    start = orm.DateField(required = False, index = False)
    end   = orm.DateField(required = False, index = False)
    
    def todate(self, v):
        return todate(v)
    
