from stdnet import orm
from stdnet.utils import encoders, todatetime, todate, missing_intervals


class HashTimeSeriesField(orm.HashField):
    '''To be used with subclasses of :class:`TimeSeriesBase`.'''
    def register_with_model(self, name, model):
        super(HashTimeSeriesField,self).register_with_model(name, model)
        self.pickler = model.converter
        
        
class TimeSeriesField(orm.MultiField):
    '''A timeseries field based on TS data structure in Redis.
To be used with subclasses of :class:`TimeSeriesBase`'''
    type = 'ts'
    
    def get_pipeline(self):
        return 'ts'
        
    def register_with_model(self, name, model):
         # must be set before calling super method
        self.pickler = model.converter
        self.value_pickler = self.value_pickler or self.default_pickler
        super(TimeSeriesField,self).register_with_model(name, model)
        
        
class TimeSeriesBase(orm.StdModel):
    '''Timeseries base model class'''
    '''Class responsable for converting Python dates into unix timestamps'''
    converter = encoders.DateTimeConverter()
    
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
        return missing_intervals(startdate, enddate, self.data_start,
                                 self.data_end, dateconverter = self.todate,
                                 parseinterval = parseinterval)        
    
    
class TimeSeries(TimeSeriesBase):
    '''Timeseries model'''
    data  = TimeSeriesField()
    
    def dates(self):
        return self.data.keys()
    
    def items(self):
        return self.data.items()
    
    def __get_start(self):
        return self.data.front()
    data_start = property(__get_start)
    
    def __get_end(self):
        return self.data.back()
    data_end = property(__get_end)
    

class DateTimeSeries(TimeSeries):
    converter = encoders.DateConverter()
    
    def todate(self, v):
        return todate(v)
    

class HashTimeSeries(TimeSeriesBase):
    '''Base abstract class for timeseries'''
    data = HashTimeSeriesField()
    data_start = orm.DateTimeField(required = False, index = False)
    data_end = orm.DateTimeField(required = False, index = False)
    
    def dates(self):
        return self.data.sortedkeys()
    
    def items(self):
        return self.data.sorteditems()
    
    def save(self, transaction = None):
        supersave = super(HashTimeSeries,self).save
        c = supersave(transaction = transaction)
        if not transaction:
            self.storestartend()
            return supersave()
        else:
            return c
    
    def storestartend(self):
        '''Store the start/end date of the timeseries'''
        dates = self.data.sortedkeys()
        if dates:
            self.data_start = dates[0]
            self.data_end   = dates[-1]
        else:
            self.data_start = None
            self.data_end   = None
    
    def fromto(self):
        if self.data_start:
            return '%s - %s' % (self.data_start.strftime('%Y %m %d'),\
                                self.data_end.strftime('%Y %m %d'))
        else:
            return ''
        
    def __unicode__(self):
        return self.fromto()


class DateHashTimeSeries(HashTimeSeries):
    converter = encoders.DateConverter()
    data_start = orm.DateField(required = False, index = False)
    data_end = orm.DateField(required = False, index = False)
    
    def todate(self, v):
        return todate(v)
    
