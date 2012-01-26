'''A Redis timeseries module base on redis time-series and
redis strings.
'''
from stdnet import orm
from stdnet.lib import skiplist
from stdnet.utils import encoders, iteritems, zip

from .encoders import ValueEncoder


class TimeseriesCache(object):
    
    def __init__(self):
        self.fields = {}
        self.delete_fields = set()
        self.deleted_timestamps = set()

    def add(self, timestamp, field, value):
        if field not in self.fields:
            self.fields[field] = skiplist()
        self.fields[field].insert(timestamp,value)
        
    def clear(self):
        self.fields.clear()
        self.delete_fields.clear()
        self.deleted_timestamps.clear()


class ColumnTS(orm.Structure):
    cache_class = TimeseriesCache
    pickler = encoders.DateTimeConverter()
    value_pickler = ValueEncoder()
    
    def fields(self):
        '''Return a tuple of ordered fields for this :class:`ColumnTS`.'''
        return self.backend_structure().fields()
    
    def numfields(self):
        '''Number of fields'''
        return self.backend_structure().numfields()
    
    def add(self, dt, *args):
        timestamp = self.pickler.dumps(dt)
        add = self.cache.add
        dump = self.value_pickler.dumps
        if len(args) == 1:
            mapping = args[0]
            if isinstance(mapping,dict):
                mapping = iteritems(mapping)
            for field,value in mapping:
                add(timestamp, field, dump(value))
        elif len(args) == 2:
            add(timestamp, args[0], dump(args[1]))
        else:
            raise TypeError('Expected a mapping or a field value pair')
        
    def update(self, mapping):
        if isinstance(mapping,dict):
            mapping = itervalues(mapping)
        add = self.add
        for dt,v in mapping:
            add(dt,v)
    
    def size(self):
        return self.backend_structure().size()
    
    def range(self, start = 0, end = -1, fields = None):
        res = self.backend_structure().range(start,end,fields)
        return self.async_handle(res, self._range)
    
    def rangebytime(self, start, end, fields = None):
        res = self.backend_structure().range(start,end,fields)
        return self.async_handle(res, self._range)
    
    # INTERNALS
    
    def _range(self, result):
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        dt = [loads(t) for t in result[0]]
        vals = {}
        for f,data in result[1]:
            vals[f] = [vloads(d) for d in data]
        return (dt,vals)
    
    
class TimeSeriesField(orm.MultiField):
    '''An experimenta timeseries field.'''
    default_value_pickler = ValueEncoder()
    
