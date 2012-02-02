'''\
Modulule containing utility classes for retrieving and displaying
Redis status and statistics.
'''
from distutils.version import StrictVersion

from stdnet.utils.structures import OrderedDict
from stdnet.utils import iteritems
from stdnet import orm

init_data = {'set':{'count':0,'size':0},
             'zset':{'count':0,'size':0},
             'list':{'count':0,'size':0},
             'hash':{'count':0,'size':0},
             'ts':{'count':0,'size':0},
             'string':{'count':0,'size':0},
             'unknown':{'count':0,'size':0}}


OBJECT_VERSION = StrictVersion('2.4.0')


__all__ = ['RedisDb',
           'RedisKey',
           'RedisDataFormatter',
           'redis_info']

    
class RedisDb(orm.ModelBase):
    
    def __init__(self, version = None, client = None, db = None, keys = None,
                 expires = None):
        self.version = version
        self.client = client
        self.id = db
        if client and db is None:
            self.id = client.db
        self.keys = keys
        self.expires = expires
    
    def delete(self):
        client = self.client
        if client:
            if client.db != self.id:
                db = client.db
                client.select(self.id)
                client.flushdb()
                client.select(db)
            else:
                client.flushdb()
        
    @property
    def db(self):
        return self.id
    
    def __unicode__(self):
        return '{0}'.format(self.id)


class RedisKeyManager(object):
    
    def all(self, db):
        keys = []
        stats = init_data.copy()
        append = keys.append
        for info in self.keys(db, None, '*'):
            append(info)
            d = stats[info.type]
            d['count'] += 1
            d['size'] += info.length
        return keys,stats
    
    def keys(self, db, keys, *patterns):
        for info in db.client.script_call('keyinfo', keys, *patterns):
            info.database = db
            yield info
            
    def delete(self, instances):
        if instances:
            keys = tuple((instance.id for instance in instances))
            return instances[0].client.delete(*keys)
    
    
class RedisKey(orm.ModelBase):
    database = None
    def __init__(self, client = None, type = None, length = 0, ttl = None,
                 encoding = None, idle = None, **kwargs):
        self.client = client
        self.type = type
        self.length = length
        self.time_to_expiry = ttl
        self.encoding = encoding
        self.idle = idle
    
    objects = RedisKeyManager()
    
    @property
    def key(self):
        return self.id
    
    def __unicode__(self):
        return self.key
    

class RedisData(list):
    
    def __init__(self, *args, **kwargs):
        self.version = kwargs.pop('version',None)
        super(RedisData,self).__init__(*args, **kwargs)
        
    def append(self, **kwargs):
        instance = RedisDb(self.version,**kwargs)
        super(RedisData,self).append(instance)
    
    @property
    def totkeys(self):
        keys = 0
        for db in self:
            keys += db.keys
        return keys


def iter_int(n,C=3,sep=','):
    c = 0
    for v in reversed(str(abs(n))):
        if c == C:
            c = 0
            yield sep
        else:
            yield v

            
def format_int(val):
    n = int(val)
    c = ''.join(reversed(list(iter_int(n))))
    if n < 0:
        c = '-{0}'.format(c)
    return c


def niceadd(l,name,value):
    if value is not None:
        l.append({'name':name,'value':value})
    

def getint(v):
    try:
        return int(v)
    except:
        return None


def get_version(info):
    if 'redis_version' in info:
        return info['redis_version']
    else:
        return info['Server']['redis_version']


class RedisDataFormatter(object):
    
    def format_bool(self, val):
        return 'yes' if val else 'no'
    
    def format_name(self, name):
        return name
    
    def format_int(self, val):
        return format_int(val)
    
    def format_date(self, dte):
        try:
            d = datetime.fromtimestamp(dte)
            return d.isoformat().split('.')[0]
        except:
            return ''
    
    def format_timedelta(self, td):
        return td
    
    
class RedisInfo(object):
    
    def __init__(self, client, version, info, formatter):
        self.client = client
        self.version = version
        self.info = info
        self._panels = OrderedDict()
        self.formatter = formatter
        self.makekeys()
        
    def panels(self):
        if not self._panels:
            self.fill()
        return self._panels
    
    def _dbs(self,keydata):
        for k in keydata:
            if k[:2] == 'db':
                try:
                    n = int(k[2:])
                except:
                    continue
                else:
                    yield k,n,keydata[k]
    
    def dbs(self,keydata):
        return sorted(self._dbs(keydata), key = lambda x : x[1])
            
    def db(self,n):
        return self.info['db{0}'.format(n)]
        
    def _makekeys(self, kdata):
        rd = RedisData(version = self.version)
        tot = 0
        databases = []
        for k,n,data in self.dbs(kdata):
            keydb = data['keys']
            rd.append(client = self.client, db = n, keys = data['keys'],
                      expires = data['expires'])
        self.databases = rd
    
    def makekeys(self):
        raise NotImplementedError
    
    def fill(self):
        raise NotImplementedError
    

class RedisInfo22(RedisInfo):
    names = ('Server','Memory','Persistence','Diskstore',
             'Replication','Clients','Stats','CPU')
    converters = {'last_save_time': ('date',None),
                  'uptime_in_seconds': ('timedelta','uptime'),
                  'uptime_in_days':None}
    
    def makekeys(self):
        if 'Keyspace' in self.info:
            return self._makekeys(self.info['Keyspace'])
        else:
            return self._makekeys(self.info)
        
    def makepanel(self, name):
        if name not in self.info:
            return
        pa = self._panels[name] = []
        nicename = self.formatter.format_name
        nicebool = self.formatter.format_bool
        boolval = (0,1)
        for k,v in iteritems(self.info[name]):
            add = True
            if k in self.converters:
                fdata = self.converters[k]
                if fdata:
                    formatter = getattr(self.formatter,
                                        'format_{0}'.format(fdata[0]))
                    k = fdata[1] or k
                    v = formatter(v)
                else:
                    add = False
            elif v in boolval:
                v = nicebool(v)
            if add:
                pa.append({'name':nicename(k),
                           'value':v})
            
    def fill(self):
        info = self.info
        for name in self.names:
            self.makepanel(name)
            
            
def redis_info(client, formatter = None):
    info = client.info()
    version = get_version(info)
    formatter = formatter or RedisDataFormatter()
    if StrictVersion(version) >= StrictVersion('2.2.0'):
        return RedisInfo22(client,version,info,formatter)
    else:
        raise NotImplementedError('Redis must be of version 2.2 or higher')
    