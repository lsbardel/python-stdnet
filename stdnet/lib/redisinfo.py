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


class RedisKeyData(orm.FakeModel):
    
    def __init__(self, key, typ, len, ttl, enc):
        self.key = key
        self.type = typ
        self.length = len
        self.time_to_expiry = ttl
        self.encoding = enc
        
    def __unicode__(self):
        return self.key
    
    def delete(self):
        pass


class RedisStats(object):
    
    def __init__(self, rpy, version = None):
        self.version = version
        self.r = rpy
        self._data = init_data.copy()

    @property
    def data(self):
        self.all()
        return self._data
      
    @property
    def keys(self):
        if not hasattr(self,'_keys'):
            self._keys = self.r.keys()
        return self._keys
    
    def size(self):
        return self.r.dbsize()
    
    def incr_count(self, t, s = 0):
        d = self._data[t]
        d['count'] += 1
        d['size'] += s
        
    def __len__(self):
        return self.size()
    
    def __iter__(self):
        return iter(self.data)
    
    def _iterate(self, data):
        get_key = self.get_key
        rpy = self.r
        for key in data:
            keys = key.decode()
            yield get_key(key)
    
    def all(self):
        '''Return a generator over info on all keys'''
        if not hasattr(self,'_all'):
            self._all = list(self._iterate(self.keys))
        return self._all
    
    def __getitem__(self, slic):
        data = self.cached_data()[slic]
        return self._iterate(data)
    
    def get_key(self, key):
        '''Retrive the type and length of a redis key.
        '''
        r = self.r
        pipe = r.pipeline()
        pipe.type(key).ttl(key)
        # Not working yet!
        #if self.version >= OBJECT_VERSION:
        #    pipe.object('encoding',key)
        tt = pipe.execute()
        typ = tt[0]
        enc = None
        if typ == 'set':
            cl = pipe.scard(key).srandmember(key).execute()
            l = cl[0]
            self.incr_count(typ,len(cl[1]))       
        elif typ == 'zset':
            cl = pipe.zcard(key).zrange(key,0,0).execute()
            l = cl[0]
            self.incr_count(typ,len(cl[1][0]))
        elif typ == 'list':
            cl = pipe.llen(key).lrange(key,0,0).execute()
            l = cl[0]
            self.incr_count(typ,len(cl[1][0]))
        elif typ == 'hash':
            l = r.hlen(key)
            self.incr_count(typ)
        elif typ == 'ts':
            l = r.execute_command('TSLEN', key)
            self.incr_count(typ)
        elif typ == 'string':
            try:
                l = r.strlen(key)
            except:
                l = None
            self.incr_count(typ)
        else:
            self.incr_count('unknown')
            typ = None
            l = None
        ttl = tt[1]
        if ttl == -1:
            ttl = False
        return RedisKeyData(key,typ,l,ttl,enc)

    
class RedisDbData(orm.FakeModel):
    
    def __init__(self, version = None, rpy = None, db = None, keys = None,
                 expires = None):
        self.version = version
        self.rpy = rpy
        self.id = db
        if rpy and db is None:
            self.id = rpy.db
        self.keys = keys
        self.expires = expires
    
    def delete(self):
        rpy = self.rpy
        if rpy:
            if rpy.db != self.id:
                db = rpy.db
                rpy.select(self.id)
                rpy.flushdb()
                rpy.select(db)
            else:
                rpy.flushdb()
        
    @property
    def db(self):
        return self.id
    
    def __unicode__(self):
        return '{0}'.format(self.id)
    
    def stats(self):
        if not hasattr(self,'_stats'):
            self._stats = RedisStats(self.rpy, self.version)
        return self._stats


class RedisData(list):
    
    def __init__(self, *args, **kwargs):
        self.version = kwargs.pop('version',None)
        super(RedisData,self).__init__(*args, **kwargs)
        
    def append(self, **kwargs):
        instance = RedisDbData(self.version,**kwargs)
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
    
    def __init__(self, rpy, version, info, formatter):
        self.rpy = rpy
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
            rd.append(rpy = self.rpy, db = n, keys = data['keys'],
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
        return self._makekeys(self.info['Keyspace'])
        
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
            
            
def redis_info(rpy, formatter = None):
    info = rpy.info()
    version = get_version(info)
    formatter = formatter or RedisDataFormatter()
    if StrictVersion(version) >= StrictVersion('2.2.0'):
        return RedisInfo22(rpy,version,info,formatter)
    else:
        raise NotImplementedError('Redis must be of version 2.2 or higher')
    