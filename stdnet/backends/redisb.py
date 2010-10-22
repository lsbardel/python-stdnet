import stdnet
from stdnet.utils import jsonPickler
from stdnet import BackendDataServer, ImproperlyConfigured, novalue
from stdnet.backends.structures import structredis

try:
    import redis
except:
    raise ImproperlyConfigured("Redis backend requires the 'redis' library. Do easy_install redis")


class BackendDataServer(stdnet.BackendDataServer):

    structure_module = structredis
    def __init__(self, name, server, params, **kwargs):
        super(BackendDataServer,self).__init__(name,
                                               params,
                                               **kwargs)
        servs = server.split(':')
        server = servs[0]
        port   = 6379
        if len(server) == 2:
            port = int(servs[1])
        self.db              = self.params.pop('db',0)
        redispy              = redis.Redis(host = server, port = port, db = self.db)
        self.redispy         = redispy
        self.execute_command = redispy.execute_command
        self.incr            = redispy.incr
        self.clear           = redispy.flushdb
        self.sinter          = redispy.sinter
        self.delete          = redispy.delete
        self.keys            = redispy.keys
    
    def __repr__(self):
        return '%s backend' % self.__name
    
    def set_timeout(self, id, timeout):
        timeout = timeout or self.default_timeout
        if timeout:
            self.execute_command('EXPIRE', id, timeout)
    
    def has_key(self, id):
        return self.execute_command('EXISTS', id)
    
    def _set(self, id, value, timeout):
        if timeout:
            return self.execute_command('SETEX', id, timeout, value)
        else:
            return self.execute_command('SET', id, value)
    
    def _get(self, id):
        return self.execute_command('GET', id)
            
    def query(self, meta, fargs, eargs):
        '''Query a model table'''
        qset = None
        if fargs:
            skeys = [meta.basekey(name,value) for name,value in fargs.iteritems()]
            qset  = self.sinter(skeys)
        if eargs:
            skeys = [meta.basekey(name,value) for name,value in fargs.iteritems()]
            eset  = self.sinter(skeys)
            if not qset:
                qset = set(hash(meta.basekey()).keys())
            return qset.difference(eset)
        else:
            if qset is None:
                return 'all'
            else:
                return qset
    
    def _set_keys(self, keys):
        items = []
        timeouts = {}
        for key,val in keys.iteritems():
            timeout = val.timeout
            if timeout:
                timeouts[key] = timeout
            items.append(key)
            items.append(val.value)
        self.execute_command('MSET', *items)
        for key,timeout in timeouts.iteritems():
            self.execute_command('EXPIRE', key, timeout)
        
    
    
