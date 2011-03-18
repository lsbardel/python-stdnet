
init_data = {'set':{'count':0,'size':0},
             'zset':{'count':0,'size':0},
             'list':{'count':0,'size':0},
             'hash':{'count':0,'size':0},
             'ts':{'count':0,'size':0},
             'string':{'count':0,'size':0},
             'unknow':{'count':0,'size':0}}


class RedisStats(object):
    
    def __init__(self, rpy):
        self.rpy = rpy
        self.data = init_data.copy()

    def keys(self):
        return self.rpy.keys()
    
    def size(self):
        return self.rpy.dbsize()
    
    def incr_count(self, t, s = 0):
        d = self.data[t]
        d['count'] += 1
        d['size'] += s

    def type_length(self, key):
        '''Retrive the type and length of a redis key.
        '''
        r = self.rpy
        pipe = r.pipeline()
        pipe.type(key).ttl(key)
        tt = pipe.execute()
        typ = tt[0]
        if typ == 'set':
            cl = pipe.scard(key).srandmember(key).execute()
            l = cl[0]
            self.incr_count(typ,len(cl[1]))       
        elif typ =='zset':
            cl = pipe.zcard(key).zrange(key,0,0).execute()
            l = cl[0]
            self.incr_count(typ,len(cl[1][0]))
        elif typ == 'list':
            l = pipe.llen(key).lrange(key,0,0)
            self.incr_count(typ,len(cl[1][0]))
        elif typ == 'hash':
            l = r.hlen(key)
            self.incr_count(typ)
        elif typ == 'ts':
            l = r.execute_command('TSLEN', key)
            self.incr_count(typ)
        elif typ == 'string':
            l = r.strlen(key)
            self.incr_count(typ)
        else:
            self.incr_count('unkown')
            l = None
        return typ,l,tt[1]

