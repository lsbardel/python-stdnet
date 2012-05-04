from datetime import date

from stdnet import odm
from stdnet.contrib.timeseries.models import TimeSeries
    

class FinanceTimeSeries(TimeSeries):
    ticker = odm.SymbolField(unique = True)
    
    def __unicode__(self):
        return '%s - %s' % (self.ticker,self.data.size())

    

if __name__ == '__main__':
    odm.register(FinanceTimeSeries)
    ts = FinanceTimeSeries(ticker = 'GOOG').save()
    ts.data[date(2010,2,25)] = 610.5
    ts.save()