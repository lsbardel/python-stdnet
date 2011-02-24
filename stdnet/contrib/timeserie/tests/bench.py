from datetime import datetime, date

from stdnet import test
from stdnet.contrib.timeserie.tests.models import TimeSeries, HashTimeSeries
from stdnet.utils import populate, todate, zip

NUM_DATES = 1000

names     = populate('string',NUM_DATES, min_len=6, max_len=15)
dates     = populate('date',NUM_DATES)
dates2    = populate('date',NUM_DATES,start=date(2009,1,1),end=date(2010,1,1))
values    = populate('float',NUM_DATES, start = 10, end = 400)
alldata   = zip(dates,values)
alldata2  = zip(dates2,values)
testdata  = dict(alldata)
testdata2 = dict(alldata2)

class UpdateTimeSerie(test.BenchMark):
    model = TimeSeries
    number = 100
    tag    = 'ts'
    def register(self):
        self.names = iter(names)
        self.orm.register(self.model)
    
    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__,NUM_DATES*self.number)
    
    def run(self):
        ts = self.model(ticker = self.names.next()).save()
        ts.data.update(testdata)
        ts.save()
        

class UpdateHash(UpdateTimeSerie):
    model = HashTimeSeries
    
    
class AddToTimeSeries(UpdateTimeSerie):
    tag   = 'ts'
    def run(self):
        ts = self.model(ticker = self.names.next()).save()
        data = ts.data
        for k,v in testdata.iteritems():
            data.add(k,v)
            data.save()
        
            
class AddToHash(AddToTimeSeries):
    model = HashTimeSeries
        