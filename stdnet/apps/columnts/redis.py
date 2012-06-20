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
        sargs = self.flat()
        if sargs:
            return self.client.script_call('timeseries_run', self.id,
                                           'session', *sargs)
        elif cache.merged_series:
            return self.client.script_call('timeseries_run', self.id,
                                           'merge', cache.merged_series)
    
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
    
    def get(self, dte):
        return self.client.script_call('timeseries_run', self.id, 'get', dte,
                                       return_type='get')
    
    def irange(self, start=0, end=-1, fields=None, **kwargs):
        fields = fields or ()
        return self.client.script_call('timeseries_run', self.id, 'irange',
                                       start, end, *fields, fields=fields,
                                       return_type='range', **kwargs)
        
    def range(self, start, end, fields=None, **kwargs):
        fields = fields or ()
        return self.client.script_call('timeseries_run', self.id, 'range',
                                       start, end, *fields, fields=fields,
                                       return_type='range', **kwargs)
    
    def pop_range(self, start, end, **kwargs):
        return self.client.script_call('timeseries_run', self.id,
                                       'pop_range', start, end,
                                       return_type='range', **kwargs)
        
    def ipop_range(self, start=0, end=-1, **kwargs):
        return self.client.script_call('timeseries_run', self.id,
                                       'ipop_range', start, end,
                                       return_type='range', **kwargs)
    
    def times(self, start, end, **kwargs):
        return self.client.script_call('timeseries_run', self.id,
                                       'times', start, end, **kwargs)
        
    def itimes(self, start=0, end=-1, **kwargs):
        return self.client.script_call('timeseries_run', self.id,
                                       'itimes', start, end, **kwargs)

    def stats(self, start, end, fields=None, **kwargs):
        fields = fields or ()
        return self.client.script_call('timeseries_run', self.id, 'stats',
                                       start, end, *fields,
                                       return_type='json', **kwargs)
        
    def istats(self, start, end, fields=None, **kwargs):
        fields = fields or ()
        return self.client.script_call('timeseries_run', self.id, 'istats',
                                       start, end, *fields,
                                       return_type='json', **kwargs)
    
    def multi_stats(self, start, end, fields, series, stats):
        return self._multi_stats(start, end, 'multi_stats', fields, series,
                                 stats)
        
    def imulti_stats(self, start, end, fields, series, stats):
        return self._multi_stats(start, end, 'imulti_stats', fields, series,
                                 stats)
    
    def merge(self, series, fields):
        all_series = []
        argv = {'series': all_series, 'fields': fields}
        for elems in series:
            ser = []
            d = {'weight': elems[0],
                 'series': ser}
            all_series.append(d)
            for ts in elems[1:]:
                ser.append(ts.backend_structure().id)
        self.instance.cache.merged_series = json.dumps(argv)
        
    def run_script(self, script_name, series, *args, **params):
        keys = (self.id,)
        if series:
            keys += tuple(series)
        if params:
            args = list(args)
            args.append(json.dumps(params))
        return self.client.script_call('timeseries_run', keys,
                                       script_name, *args)
    
    ###############################################################  INTERNALS
    def flat(self):
        cache = self.instance.cache
        if cache.deleted_timestamps or cache.delete_fields or cache.fields:
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
            return args
    
    def _multi_stats(self, start, end, command, fields, series, stats):
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
            argv.append(fields)
        fields = json.dumps(argv)
        return self.client.script_call('timeseries_run', keys, command,
                                       start, end, fields, return_type='json')
        

# Add the redis structure to the struct map in the backend class
redisb.BackendDataServer.struct_map['columnts'] = RedisColumnTS


##############################################################    SCRIPT
class timeseries_run(redis.RedisScript):
    script = (redis.read_lua_file('tabletools'),
              redis.read_lua_file('columnts.columnts'),
              redis.read_lua_file('columnts.stats'),
              redis.read_lua_file('columnts.runts'))
    
    def callback(self, request, response, args, fields=None,
                 return_type=None, **options):
        encoding = request.client.encoding
        if return_type == 'range':
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
        elif return_type == 'json':
            return json.loads(response.decode(encoding))
        elif return_type == 'get' and response:
            rfields = (f.decode(encoding) for f in response[::2])
            return zip(rfields, response[1::2])
        else:
            return response
    
    
        
    