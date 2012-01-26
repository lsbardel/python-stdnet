import os

from stdnet.backends import redisb
from stdnet.lib import read_lua_file, RedisScript


class RedisColumnTS(redisb.TS):
    
    def flush(self):
        cache = self.instance.cache
        keysargs = self.flat()
        if keysargs:
            keys, args = keysargs
            return self.client.script_call('timeseries_session', keys, *args)
                    
    def fields(self):
        '''Return a tuple of ordered fields for this :class:`ColumnTS`.'''
        prefix = self.id + ':field:'
        start = len(prefix)
        return tuple(sorted((f[start:] for f in self.client.keys(prefix+'*'))))
    
    def numfields(self):
        '''Number of fields'''
        return self.client.script_call('countpattern',self.id + ':field:*')
    
    def range(self, start = 0, end = -1, fields = None):
        fields = fields or ()
        return self.client.script_call('timeseries_query', self.id,
                                       'tsrange',
                                       start, end, len(fields), *fields)
        
    def rangebytime(self, start, end, fields = None):
        fields = fields or ()
        start = self.pickler.dumps(start)
        end = self.pickler.dumps(end)
        return self.client.script_call('timeseries_query', self.id,
                                      'tsrangebytime',
                                       start, end, len(fields), *fields)
    
    def add(self, dt, field, value):
        timestamp = self.pickler.dumps(dt)
        self.cache.add(timestamp, field, self.value_pickler.dumps(value))
        
    def flat(self):
        cache = self.instance.cache
        if cache.deleted_timestamps or cache.delete_fields or cache.fields:
            # timestamps to delete
            keys = (self.id, self.id + ':field:*')
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
    
            
redisb.struct_map['columnts'] = RedisColumnTS

script_path = os.path.join(os.path.split(os.path.abspath(__file__))[0],'lua')

class timeseries_session(RedisScript):
    script = read_lua_file('session.lua',script_path)
    
    
class timeseries_query(RedisScript):
    script = (read_lua_file('utils/table.lua'),
              read_lua_file('query.lua',script_path))
    
    def callback(self, request, response, args, **options):
        if isinstance(response,Exception):
            raise response
        encoding = request.client.encoding
        fields = [f.decode(encoding) for f in response[1::2]]
        return response[0], zip(fields,response[2::2]) 
        
    