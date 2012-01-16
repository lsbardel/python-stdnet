import os
from struct import pack, unpack

from stdnet import orm, getdb
from stdnet.lib import skiplist, read_lua_file, RedisScript
from stdnet.utils import encoders

##########################################################
# flags
#
#    \x00    nan
#    \x01    int
#    \x02    float
#    \x03    small string (len <= 8)
#    \x04    big string (len > 8)
##########################################################

script_path = os.path.join(os.path.split(os.path.abspath(__file__))[0],'lua')
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
        
    def flush(self):
        self.fields.clear()
        self.delete_fields.clear()
        self.deleted_timestamps.clear()
        
    def flat(self):
        if self.deleted_timestamps or self.delete_fields or self.fields:
            args = [len(self.deleted_timestamps)]
            args.extend(self.deleted_timestamps)
            args.append(len(self.delete_fields))
            args.extend(self.delete_fields)
            for field in self.fields:
                val = self.fields[field]
                args.append(field)
                args.append(len(val))
                args.extend(val.flat())
            self.flush()
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
            
    def loads(self, value):
        flag = value[0]
        if flag == b'\x00':
            return nan 
        elif flag == b'\x01':
            return bin_to_int(value[1:])
        elif flag == b'\x02':
            return bin_to_float(value[1:])
        else:
            return super(ValueEncoder,self).loads(value[1:])
        
        
class TS(orm.Structure):
    cache_class = TimeseriesCache
    pickler = encoders.DateTimeConverter()
    value_pickler = ValueEncoder()
    
    @property
    def cache(self):
        if self._cache is None:
            self._cache = self.cache_class()
        return self._cache
    
    @property
    def fields_id(self):
        return self.id + ':fields'
    
    def fields(self):
        '''Return the list of fields for this timeseries'''
        return self.client.smembers(self.fields_id)
    
    def numfields(self):
        '''Return the list of fields for this timeseries'''
        return self.client.scard(self.fields_id)
    
    def add(self, dt, field, value):
        timestamp = self.pickler.dumps(dt)
        self.cache.add(timestamp, field, self.value_pickler.dumps(value))
    
    def size(self):
        return self.client.zcard(self.id)
    
    def commit(self):
        args = self.cache.flat()
        if args:
            self.client.script_call('timeseries_session', self.id, *args)
    
    
class TimeSeriesField(orm.MultiField):
    '''An experimenta timeseries field.'''
    default_value_pickler = ValueEncoder()
    
    def register_with_model(self, name, model):
         # must be set before calling super method
        self.pickler = model.converter
        self.value_pickler = self.value_pickler or self.default_pickler
        super(TimeSeriesField,self).register_with_model(name, model)
        
    
class TimeseriesSessionScript(RedisScript):
    script = read_lua_file('session.lua',script_path)
    