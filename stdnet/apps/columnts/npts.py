from stdnet.apps.columnts import models as columnts

import numpy as ny

from dynts import timeseries, tsname


class ColumnTS(columnts.ColumnTS):
    '''Integrate stdnet timeseries with dynts_ TimeSeries'''
    
    def front(self, *fields):
        '''Return the front pair of the structure'''
        ts = self.irange(0, 0, fields = fields)
        if ts:
            return ts.start(),ts[0]
    
    def back(self, *fields):
        '''Return the back pair of the structure'''
        ts = self.irange(-1, -1, fields = fields)
        if ts:
            return ts.end(),ts[0]
        
    def load_data(self, result):
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        dt,va = result
        if result[0]:
            dates = ny.array([loads(t) for t in dt])
            # fromiter does not work for object arrays
            #dates = ny.fromiter((loads(t) for t in dt),
            #                    self.pickler.type,
            #                    len(dt)) 
            fields = []
            vals = []
            for f,data in va:
                fields.append(f)
                vals.append([vloads(d) for d in data])
            values = ny.array(vals).transpose()
            name = tsname(*fields)
        else:
            name = None
            dates = None
            values = None
        return timeseries(name = name,
                          date = dates,
                          data = values)
    
    def _get(self, result):
        ts = self.load_data(result)
        return ts.front()
    
    
class TimeSeriesField(columnts.TimeSeriesField):
    
    def structure_class(self):
        return ColumnTS
    