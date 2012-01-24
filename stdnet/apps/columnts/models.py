'''A Redis timeseries module base on redis time-series and
redis strings.
'''
from struct import pack, unpack

from stdnet import orm
from stdnet.lib import skiplist
from stdnet.utils import encoders, iteritems, zip, ispy3k

##########################################################
# flags
#
#    \x00    nan
#    \x01    int
#    \x02    float
#    \x03    small string (len <= 8)
#    \x04    big string (len > 8)
##########################################################

nil = b'\x00'*9
nil4 = b'\x00'*4
nan = float('nan')
float_to_bin = lambda f : pack('>d', f)
bin_to_float = lambda f : unpack('>d', f)[0]
int_to_bin = lambda f : pack('>i', f) + nil4
bin_to_int = lambda f : unpack('>i', f[:4])[0]


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
        
    def flat(self):
        if self.deleted_timestamps or self.delete_fields or self.fields:
            # timestamps to delete
            args = [len(self.deleted_timestamps)]
            args.extend(self.deleted_timestamps)
            # fields to delete
            args.append(len(self.delete_fields))
            args.extend(self.delete_fields)
            # For each field we have: field_name, len(vals), [t1,v1,t2,v2,...]
            for field in self.fields:
                val = self.fields[field]
                args.append(field)
                args.append(len(val))
                args.extend(val.flat())
            return args
        
        
class ValueEncoder(encoders.Default):
        
    def dumps(self, value):
        if value is None:
            return nil
        try:
            value = float(value)
            if value != value:
                return nil
            elif value == int(value):
                return b'\x01'+int_to_bin(int(value))
            else:
                return b'\x02'+float_to_bin(value)
        except ValueError:
            value = super(ValueEncoder,self).dumps(value)
            if len(value) <= 8:
                return b'\x03'+value
            else:
                val = b'\x04'+sha1(value).hexdigest(value)[:8].encode('utf-8')
                return val+value
    
    if ispy3k:
        
        def loads(self, value):
            flag = value[0]
            if flag == 0:
                return nan 
            elif flag == 1:
                return bin_to_int(value[1:])
            elif flag == 2:
                return bin_to_float(value[1:])
            else:
                return super(ValueEncoder,self).loads(value[1:])
    
    else:
        
        def loads(self, value):
            flag = ord(value[0])
            if flag == 0:
                return nan 
            elif flag == 1:
                return bin_to_int(value[1:])
            elif flag == 2:
                return bin_to_float(value[1:])
            else:
                return super(ValueEncoder,self).loads(value[1:])


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
    
