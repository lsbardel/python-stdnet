import os
import json

from stdnet.backends import redisb
from stdnet.utils.encoders import safe_number
from stdnet.lib import redis


class RedisColumnTS(redisb.Zset):
    '''Redis backend for :class:`ColumnTS`'''
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
    
    def _iter(self):
        return iter(self.irange(novalues=True))
    
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
    
    def irange(self, start = 0, end = -1, fields=None, novalues=False,
               delete=False, **kwargs):
        noval = 1 if novalues else 0
        fields = fields or ()
        delete = 1 if delete else 0
        return self.client.script_call(
                        'timeseries_query', self.id, 'zrange',
                        start, end, noval, delete, len(fields),
                        *fields, fields = fields, novalues=novalues)
        
    def range(self, start, end, fields=None, novalues=False, **kwargs):
        noval = 1 if novalues else 0
        fields = fields or ()
        return self.client.script_call(
                        'timeseries_query', self.id, 'zrangebyscore',
                        start, end, noval, 0, len(fields), *fields,
                        fields = fields, novalues = novalues)
        
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
        
    def istats(self, start, end, fields = None):
        fields = fields or ()
        return self.client.script_call('timeseries_stats', self.id,
                'zrange', start, end, 'uni', len(fields), *fields)

    def stats(self, start, end, fields = None):
        fields = fields or ()
        return self.client.script_call('timeseries_stats', self.id,
                'zrangebyscore', start, end, 'uni', len(fields), *fields)
        
    def imulti_stats(self, start, end, fields, series, stats):
        return self._multi_stats('zrange', start, end, fields, series, stats)
    
    def multi_stats(self, start, end, fields, series, stats):
        return self._multi_stats('zrangebyscore', start, end, fields, series,
                                 stats)
    
    def _multi_stats(self, command, start, end, fields, series, stats):
        all = [(self.id,fields)]
        if series:
            all.extend(((ts.backend_structure().id, fields)\
                            for ts,fields in series))
        keys = []
        argv = []
        for s in all:
            if not len(s) == 2:
                raise ValueError('Series must be a list of two elements tuple')
            id, fields = s
            keys.append(id)
            fields = fields if fields is not None else ()
            argv.append(len(fields))
            argv.extend(fields)
        if stats:
            argv.extend(stats)
        return self.client.script_call('timeseries_stats', keys,
                command, start, end, 'multi', *argv)
        

# Add the redis structure to the struct map in the backend class
redisb.BackendDataServer.struct_map['columnts'] = RedisColumnTS


##############################################################    SCRIPTS

class timeseries_session(redis.RedisScript):
    script = (redis.read_lua_file('tabletools'),
              redis.read_lua_file('columnts.columnts'),
              redis.read_lua_file('columnts.session'))
    

class timeseries_merge(redis.RedisScript):
    script = (redis.read_lua_file('tabletools'),
              redis.read_lua_file('columnts.columnts'),
              redis.read_lua_file('columnts.merge'))
        
    
class timeseries_query(redis.RedisScript):
    '''Lua Script for retrieving data from the remote timeseries'''
    script = (redis.read_lua_file('tabletools'),
              redis.read_lua_file('columnts.columnts'),
              redis.read_lua_file('columnts.query'))
    
    def callback(self, request, response, args, fields = None, novalues = False,
                 **options):
        if novalues:
            return response
        encoding = request.client.encoding
        rfields = (f.decode(encoding) for f in response[1::2])
        data = dict(zip(rfields, response[2::2]))
        newdata = []
        if fields:
            for field in fields:
                value = data.pop(field,None)
                if value is not None:
                    newdata.append((field,value))
        for field in sorted(data):
            newdata.append((field,data[field]))
        return response[0], newdata
        

class timeseries_stats(redis.RedisScript):
    script = (redis.read_lua_file('tabletools'),
              redis.read_lua_file('columnts.columnts'),
              redis.read_lua_file('columnts.stats'))
    
    def callback(self, request, response, args, **options):
        encoding = request.client.encoding
        if response:
            return json.loads(response.decode(encoding))
    