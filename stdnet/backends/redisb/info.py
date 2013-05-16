'''\
Modulule containing utility classes for retrieving and displaying
Redis status and statistics.
'''
from datetime import datetime
from copy import copy

from stdnet.utils.structures import OrderedDict
from stdnet.utils.async import async
from stdnet.utils import iteritems, format_int
from stdnet import odm

from .extensions import RedisScript, read_lua_file

init_data = {'set': {'count': 0, 'size': 0},
             'zset': {'count': 0, 'size': 0},
             'list': {'count': 0, 'size': 0},
             'hash': {'count': 0, 'size': 0},
             'ts': {'count': 0, 'size': 0},
             'string': {'count': 0, 'size': 0},
             'unknown': {'count': 0, 'size': 0}}


__all__ = ['RedisDb', 'RedisKey', 'RedisDataFormatter']


class keyinfo(RedisScript):
    script = read_lua_file('commands.keyinfo')
    
    def preprocess_args(self, client, args):
        if args and client.prefix:
            a = ['%s%s' % (client.prefix, args[0])]
            a.extend(args[1:])
            args = tuple(a)
        return args
    
    def callback(self, response, redis_client=None, **options):
        client = redis_client
        if client.is_pipeline:
            client = client.client
        encoding = client.connection_pool.encoding
        all_keys = []
        for key, typ, length, ttl, enc, idle in response:
            key = key.decode(encoding)[len(client.prefix):]
            key = RedisKey(key=key, client=client,
                           type=typ.decode(encoding),
                           length=length,
                           ttl=ttl if ttl != -1 else False,
                           encoding=enc.decode(encoding),
                           idle=idle)
            all_keys.append(key)
        return all_keys


def parse_info(response):
    '''Parse the response of Redis's INFO command into a Python dict.
In doing so, convert byte data into unicode.'''
    info = {}
    response = response.decode('utf-8')
    def get_value(value):
        if ',' and '=' not in value:
            return value
        sub_dict = {}
        for item in value.split(','):
            k, v = item.split('=')
            try:
                sub_dict[k] = int(v)
            except ValueError:
                sub_dict[k] = v
        return sub_dict
    data = info
    for line in response.splitlines():
        keyvalue = line.split(':')
        if len(keyvalue) == 2:
            key,value = keyvalue
            try:
                data[key] = int(value)
            except ValueError:
                data[key] = get_value(value)
        else:
            data = {}
            info[line[2:]] = data
    return info


class RedisDbQuery(odm.QueryBase):
    
    @property
    def client(self):
        return self.session.router[self.model].backend.client
    
    @async()
    def items(self):
        client = self.client
        info = yield client.info()
        rd = []
        for n, data in self.keyspace(info):
            rd.append(self.instance(n, data))
        yield rd
    
    def get(self, db=None):
        if db is not None:
            info = yield self.client.info()
            data = info.get('db%s' % db)
            if data:
                yield self.instance(db, data)
    
    def keyspace(self, info):
        n = 0
        keyspace = info['Keyspace']
        while keyspace:
            info = keyspace.pop('db%s' % n, None)
            if info:
                yield n, info
            n += 1
    
    def instance(self, db, data):
        rdb = self.model(db=int(db), keys=data['keys'], expires=data['expires'])
        rdb.session = self.session
        return rdb


class RedisDbManager(odm.Manager):
    '''Handler for gathering information from redis.'''
    names = ('Server','Memory','Persistence',
             'Replication','Clients','Stats','CPU')
    converters = {'last_save_time': ('date', None),
                  'uptime_in_seconds': ('timedelta', 'uptime'),
                  'uptime_in_days': None}
    
    query_class = RedisDbQuery
    
    def __init__(self, *args, **kwargs):
        self.formatter = kwargs.pop('formatter', RedisDataFormatter())
        self._panels = OrderedDict()
        super(RedisDbManager, self).__init__(*args, **kwargs)
        
    @property
    def client(self):
        return self.backend.client
    
    @async()
    def panels(self):
        info = yield self.client.info()
        panels = {}
        for name in self.names:
            val = self.makepanel(name, info)
            if val:
                panels[name] = val
        yield panels
    
    def makepanel(self, name, info):
        if name not in info:
            return
        pa = []
        nicename = self.formatter.format_name
        nicebool = self.formatter.format_bool
        boolval = (0,1)
        for k,v in iteritems(info[name]):
            add = True
            if k in self.converters or isinstance(v,int):
                fdata = self.converters.get(k,('int',None))
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
        return pa
    
    def delete(self, instance):
        '''Delete an instance'''
        client = None
        flushdb(self.client) if flushdb else self.client.flushdb()
    

class KeyQuery(odm.QueryBase):
    '''A lazy query for keys in a redis database.'''
    db = None    
    def count(self):
        return self.db.client.countpattern(self.pattern)
    
    def filter(self, db=None):
        self.db=db
        return self
    
    def all(self):
        return list(self)
    
    def delete(self):
        return self.db.client.delpattern(self.pattern)
        
    def __len__(self):
        return self.count()
    
    def __getitem__(self, slic):
        o = copy(self)
        if isinstance(slic, slice):
            o.slice = slic
            return o.all()
        else:
            return self[slic:slic+1][0]
    
    def __iter__(self):
        keys = []
        db = self.db
        c = db.client
        if self.slice:
            start, num = self.get_start_num(self.slice)
            qs = c.execute_script('keyinfo', (), self.pattern, start, num)
        else:
            qs = c.execute_script('keyinfo', (), self.pattern)
        for q in qs:
            q.database = db
            yield q
    
    def get_start_num(self, slic):
        start, step, stop = slic.start, slic.step, slic.stop
        N = None
        if stop is None or stop < 0:
            N = self.count()
            stop = stop or 0
            stop += N
        start = start or 0
        if start < 0:
            if N is None:
                N = self.count()
            start += N
        return start+1, stop-start
                            

class RedisKeyManager(odm.Manager):
    query_class = KeyQuery
            
    def delete(self, instances):
        if instances:
            keys = tuple((instance.id for instance in instances))
            return instances[0].client.delete(*keys)
    

class RedisDb(odm.StdModel):
    db = odm.IntegerField(primary_key=True)
        
    manager_class = RedisDbManager
    
    def __unicode__(self):
        return '%s' % self.db
    
    class Meta:
        attributes = ('keys', 'expires')
        

class RedisKey(odm.StdModel):
    key = odm.SymbolField(primary_key=True)
    db = odm.ForeignKey(RedisDb, related_name='all_keys')
    
    manager_class = RedisKeyManager
    
    def __unicode__(self):
        return self.key
    
    class Meta:
        attributes = 'type', 'length', 'ttl', 'encoding', 'idle', 'client'
    

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
    