import os

from stdnet import orm
from stdnet.backends import redisb
from stdnet.lib import redis


class RedisColumnTS(redisb.TS):
    
    @property
    def fieldsid(self):
        return self.id + ':fields'
    
    def fieldid(self, field):
        return self.id + ':field:' + field
    
    def flush(self):
        cache = self.instance.cache
        keysargs = self.flat()
        if keysargs:
            keys, args = keysargs
            return self.client.script_call('timeseries_session', keys, *args)
        elif cache.merged_series:
            keys, args = cache.merged_series
            return self.client.script_call('timeseries_merge', keys, *args)
    
    def allkeys(self):
        return self.client.keys(self.id + '*')
    
    def fields(self):
        '''Return a tuple of ordered fields for this :class:`ColumnTS`.'''
        key = self.id + ':fields'
        encoding = self.client.encoding
        return tuple(sorted((f.decode(encoding) \
                             for f in self.client.smembers(key))))
        
    def field(self, field):
        '''Fetch an entire row field string from redis'''
        return self.client.get(self.fieldid(field))
        
    
    def numfields(self):
        '''Number of fields'''
        return self.client.scard(self.fieldsid)
    
    def irange(self, start = 0, end = -1, fields = None, delete = False):
        fields = fields or ()
        delete = 1 if delete else 0
        return self.client.script_call('timeseries_query', self.id,
                                       'tsrange',
                                       start, end, delete, len(fields), *fields)
        
    def range(self, start, end, fields = None):
        fields = fields or ()
        return self.client.script_call('timeseries_query', self.id,
                                      'tsrangebytime',
                                       start, end, 0, len(fields), *fields)
    
    def add(self, dt, field, value):
        timestamp = self.pickler.dumps(dt)
        self.cache.add(timestamp, field, self.value_pickler.dumps(value))
        
    def flat(self):
        cache = self.instance.cache
        if cache.deleted_timestamps or cache.delete_fields or cache.fields:
            # timestamps to delete
            keys = (self.id, self.id + '*')
            args = [len(cache.deleted_timestamps)]
            args.extend(cache.deleted_timestamps)
            # fields to delete
            args.append(len(cache.delete_fields))
            args.extend(cache.delete_fields)
            # For each field we have: field_name, len(vals), [t1,v1,t2,v2,...]
            for field in cache.fields:
                val = cache.fields[field]
                args.append(field)
                args.append(len(val))
                args.extend(val.flat())
            return keys, args
    
    def merge(self, series, fields):
        argv = [len(series)]
        keys = [self.id]
        for elems in series:
            argv.append(elems[0])   # add weight
            tss = elems[1:]
            argv.append(len(tss))
            for ts in tss:
                k = ts.backend_structure().id
                keys.append(k)
                argv.append(k)
        argv.extend(fields)
        self.instance.cache.merged_series = (keys,argv)
        
    def stats(self, start, end, fields = None):
        fields = fields or ()
        return self.client.script_call('timeseries_stats', self.id, start, end,
                                       len(fields), *fields)
        
            
redisb.struct_map['columnts'] = RedisColumnTS

script_path = os.path.join(os.path.split(os.path.abspath(__file__))[0],'lua')

class timeseries_session(redis.RedisScript):
    script = (redis.read_lua_file('utils/table.lua'),
              redis.read_lua_file('columnts.lua',script_path),
              redis.read_lua_file('session.lua',script_path))
    

class timeseries_merge(redis.RedisScript):
    script = (redis.read_lua_file('utils/table.lua'),
              redis.read_lua_file('columnts.lua',script_path),
              redis.read_lua_file('merge.lua',script_path))
    
    def callback(self, request, response, args, **options):
        if isinstance(response,Exception):
            raise response
        
    
class timeseries_query(redis.RedisScript):
    script = (redis.read_lua_file('utils/table.lua'),
              redis.read_lua_file('columnts.lua',script_path),
              redis.read_lua_file('query.lua',script_path))
    
    def callback(self, request, response, args, **options):
        if isinstance(response,Exception):
            raise response
        encoding = request.client.encoding
        fields = (f.decode(encoding) for f in response[1::2])
        return response[0], tuple(zip(fields,response[2::2]))
        

class timeseries_stats(redis.RedisScript):
    script = (redis.read_lua_file('utils/table.lua'),
              redis.read_lua_file('columnts.lua',script_path),
              redis.read_lua_file('stats.lua',script_path))
    
    def callback(self, request, response, args, **options):
        encoding = request.client.encoding
        result = dict(redis.pairs_to_dict(response,encoding))
        if result:
            result['stats'] =\
                dict(redis.pairs_to_dict(result['stats'],encoding))
        return result
        
