import os
from hashlib import sha1
from collections import namedtuple
from datetime import datetime
from copy import copy

from stdnet.utils.structures import OrderedDict
from stdnet.utils import iteritems, format_int
from stdnet import odm

try:
    import redis
except ImportError:     # pragma    nocover
    from stdnet import ImproperlyConfigured
    raise ImproperlyConfigured('Redis backend requires redis python client')

from redis.client import BasePipeline

RedisError = redis.RedisError
p = os.path
DEFAULT_LUA_PATH = p.join(p.dirname(p.dirname(p.abspath(__file__))), 'lua')
redis_connection = namedtuple('redis_connection', 'address db')

###########################################################
#    GLOBAL REGISTERED SCRIPT DICTIONARY
all_loaded_scripts = {}
_scripts = {}


def registered_scripts():
    return tuple(_scripts)


def get_script(script):
    return _scripts.get(script)
###########################################################


def script_callback(response, script=None, **options):
    if script:
        return script.callback(response, **options)
    else:
        return response


def read_lua_file(dotted_module, path=None, context=None):
    '''Load lua script from the stdnet/lib/lua directory'''
    path = path or DEFAULT_LUA_PATH
    bits = dotted_module.split('.')
    bits[-1] += '.lua'
    name = os.path.join(path, *bits)
    with open(name) as f:
        data = f.read()
    if context:
        data = data.format(context)
    return data


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
            key, value = keyvalue
            try:
                data[key] = int(value)
            except ValueError:
                data[key] = get_value(value)
        else:
            data = {}
            info[line[2:]] = data
    return info


def dict_update(original, data):
    target = original.copy()
    target.update(data)
    return target


class RedisExtensionsMixin(object):

    '''Extension for Redis clients.
    '''
    prefix = ''
    RESPONSE_CALLBACKS = dict_update(
        redis.StrictRedis.RESPONSE_CALLBACKS,
        {'EVALSHA': script_callback,
         'INFO': parse_info}
    )

    @property
    def is_async(self):
        return False

    @property
    def is_pipeline(self):
        return False

    def address(self):
        '''Address of redis server.
        '''
        raise NotImplementedError

    def execute_script(self, name, keys, *args, **options):
        '''Execute a registered lua script at ``name``.

        The script must be implemented via subclassing :class:`RedisScript`.

        :param name: the name of the registered script.
        :param keys: tuple/list of keys pased to the script.
        :param args: argument passed to the script.
        :param options: key-value parameters passed to the
            :meth:`RedisScript.callback` method once the script has finished
            execution.
        '''
        script = get_script(name)
        if not script:
            raise RedisError('No such script "%s"' % name)
        address = self.address()
        if address not in all_loaded_scripts:
            all_loaded_scripts[address] = set()
        loaded = all_loaded_scripts[address]
        toload = script.required_scripts.difference(loaded)
        for name in toload:
            s = get_script(name)
            self.script_load(s.script)
        loaded.update(toload)
        return script(self, keys, args, options)

    def countpattern(self, pattern):
        '''delete all keys matching *pattern*.
        '''
        return self.execute_script('countpattern', (), pattern)

    def delpattern(self, pattern):
        '''delete all keys matching *pattern*.
        '''
        return self.execute_script('delpattern', (), pattern)

    def zdiffstore(self, dest, keys, withscores=False):
        '''Compute the difference of multiple sorted.

        The difference of sets specified by ``keys`` into a new sorted set
        in ``dest``.
        '''
        keys = (dest,) + tuple(keys)
        wscores = 'withscores' if withscores else ''
        return self.execute_script('zdiffstore', keys, wscores,
                                   withscores=withscores)

    def zpopbyrank(self, name, start, stop=None, withscores=False, desc=False):
        '''Pop a range by rank.
        '''
        stop = stop if stop is not None else start
        return self.execute_script('zpop', (name,), 'rank', start,
                                   stop, int(desc), int(withscores),
                                   withscores=withscores)

    def zpopbyscore(self, name, start, stop=None, withscores=False,
                    desc=False):
        '''Pop a range by score.
        '''
        stop = stop if stop is not None else start
        return self.execute_script('zpop', (name,), 'score', start,
                                   stop, int(desc), int(withscores),
                                   withscores=withscores)


class RedisScriptMeta(type):

    def __new__(cls, name, bases, attrs):
        super_new = super(RedisScriptMeta, cls).__new__
        abstract = attrs.pop('abstract', False)
        new_class = super_new(cls, name, bases, attrs)
        if not abstract:
            self = new_class(new_class.script, new_class.__name__)
            _scripts[self.name] = self
        return new_class


class RedisScript(RedisScriptMeta('_RS', (object,), {'abstract': True})):

    '''Class which helps the sending and receiving lua scripts.

    It uses the ``evalsha`` command.

    .. attribute:: script

        The lua script to run

    .. attribute:: required_scripts

        A list/tuple of other :class:`RedisScript` names required by this
        script to properly execute.

    .. attribute:: sha1

        The SHA-1_ hexadecimal representation of :attr:`script` required by the
        ``EVALSHA`` redis command. This attribute is evaluated by the library,
        it is not set by the user.

    .. _SHA-1: http://en.wikipedia.org/wiki/SHA-1
    '''
    abstract = True
    script = None
    required_scripts = ()

    def __init__(self, script, name):
        if isinstance(script, (list, tuple)):
            script = '\n'.join(script)
        self.__name = name
        self.script = script
        rs = set((name,))
        rs.update(self.required_scripts)
        self.required_scripts = rs

    @property
    def name(self):
        return self.__name

    @property
    def sha1(self):
        if not hasattr(self, '_sha1'):
            self._sha1 = sha1(self.script.encode('utf-8')).hexdigest()
        return self._sha1

    def __repr__(self):
        return self.name if self.name else self.__class__.__name__
    __str__ = __repr__

    def preprocess_args(self, client, args):
        return args

    def callback(self, response, **options):
        '''Called back after script execution.

        This is the only method user should override when writing a new
        :class:`RedisScript`. By default it returns ``response``.

        :parameter response: the response obtained from the script execution.
        :parameter options: Additional options for the callback.
        '''
        return response

    def __call__(self, client, keys, args, options):
        args = self.preprocess_args(client, args)
        numkeys = len(keys)
        keys_args = tuple(keys) + args
        options.update({'script': self, 'redis_client': client})
        return client.execute_command('EVALSHA', self.sha1, numkeys,
                                      *keys_args, **options)


############################################################################
##    BATTERY INCLUDED REDIS SCRIPTS
############################################################################
class countpattern(RedisScript):
    script = '''\
return # redis.call('keys', ARGV[1])
'''

    def preprocess_args(self, client, args):
        if args and client.prefix:
            args = tuple(('%s%s' % (client.prefix, a) for a in args))
        return args


class delpattern(countpattern):
    script = '''\
local n = 0
for i,key in ipairs(redis.call('keys', ARGV[1])) do
  n = n + redis.call('del', key)
end
return n
'''


class zpop(RedisScript):
    script = read_lua_file('commands.zpop')

    def callback(self, response, withscores=False, **options):
        if not response or not withscores:
            return response
        return zip(response[::2], map(float, response[1::2]))


class zdiffstore(RedisScript):
    script = read_lua_file('commands.zdiffstore')


class move2set(RedisScript):
    script = (read_lua_file('commands.utils'),
              read_lua_file('commands.move2set'))


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
        encoding = 'utf-8'
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


###############################################################################
##  key info models

class RedisDbQuery(odm.QueryBase):

    @property
    def client(self):
        return self.session.router[self.model].backend.client

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
        rdb = self.model(db=int(db), keys=data['keys'],
                         expires=data['expires'])
        rdb.session = self.session
        return rdb


class RedisDbManager(odm.Manager):

    '''Handler for gathering information from redis.'''
    names = ('Server', 'Memory', 'Persistence',
             'Replication', 'Clients', 'Stats', 'CPU')
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
        boolval = (0, 1)
        for k, v in iteritems(info[name]):
            add = True
            if k in self.converters or isinstance(v, int):
                fdata = self.converters.get(k, ('int', None))
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
                pa.append({'name': nicename(k),
                           'value': v})
        return pa

    def delete(self, instance):
        '''Delete an instance'''
        flushdb(self.client) if flushdb else self.client.flushdb()


class KeyQuery(odm.QueryBase):

    '''A lazy query for keys in a redis database.'''
    db = None

    def count(self):
        return self.db.client.countpattern(self.pattern)

    def filter(self, db=None):
        self.db = db
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
            return self[slic:slic + 1][0]

    def __iter__(self):
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
        return start + 1, stop - start


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
