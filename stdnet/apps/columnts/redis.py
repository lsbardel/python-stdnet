import os

from stdnet.backends import redisb
from stdnet.lib import read_lua_file, RedisScript


class RedisColumnTS(redisb.TS):
    
    def flush(self):
        cache = self.instance.cache
        args = cache.flat()
        if args:
            return self.client.script_call('timeseries_session', self.id, *args)
                    
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
        
    