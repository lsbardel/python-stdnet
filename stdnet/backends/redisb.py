import stdnet
from stdnet.utils import jsonPickler, iteritems, to_string
from stdnet import BackendDataServer, ImproperlyConfigured
from stdnet.backends.structures import structredis
from stdnet.lib import redis


class BackendDataServer(stdnet.BackendDataServer):

    structure_module = structredis
    def __init__(self, name, server, params, **kwargs):
        super(BackendDataServer,self).__init__(name,
                                               params,
                                               **kwargs)
        servs = server.split(':')
        server = servs[0]
        port   = 6379
        if len(servs) == 2:
            port = int(servs[1])
        self.db              = self.params.pop('db',0)
        redispy              = redis.Redis(host = server, port = port, db = self.db)
        self.redispy         = redispy
        self.execute_command = redispy.execute_command
        self.incr            = redispy.incr
        self.clear           = redispy.flushdb
        self.sinter          = redispy.sinter
        self.sdiff           = redispy.sdiff
        self.sinterstore     = redispy.sinterstore
        self.sunionstore     = redispy.sunionstore
        self.delete          = redispy.delete
        self.keys            = redispy.keys
    
    def __repr__(self):
        r = self.redispy
        return '%s db %s on %s:%s' % (self.__name,r.db,r.host,r.port)
    
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
            
    def query(self, meta, fargs, eargs, filter_sets = None):
        raise NotImplementedError
        
    def get_object(self, meta, name, value):
        raise NotImplementedError
    
    def save_object(self, obj, transaction = None):
        raise NotImplementedError
        
    def flush(self, meta, count):
        '''Flush all model keys from the database'''
        if count is not None:
            count[str(meta)] = meta.table().size()
        # This should be a lua script or pipeline?
        keys = self.keys(meta.basekey()+b'*')
        if keys:
            self.delete(*keys)
