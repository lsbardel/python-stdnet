import os
from hashlib import sha1
from collections import namedtuple

try:
    import redis
except ImportError:
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


class RedisExtensionsMixin(object):
    '''StdnetExtensions for Redis clients.
    '''
    prefix = ''
    
    @property
    def is_pipeline(self):
        return False

    def address(self):
        '''Address of redis server.
        '''
        raise NotImplementedError
        
    def execute_script(self, name, keys, *args, **options):
        '''Execute a registered lua script at ``name``. The script must
        be implemented via subclassing :class:`RedisScript`.
        
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

    def zpopbyscore(self, name, start, stop=None, withscores=False, desc=False):
        '''Pop a range by score.
        '''
        stop = stop if stop is not None else start
        return self.execute_script('zpop', (name,), 'score', start,
                                   stop, int(desc), int(withscores),
                                   withscores=withscores)
    

class RedisScriptMeta(type):
    
    def __new__(cls, name, bases, attrs):
        super_new = super(RedisScriptMeta, cls).__new__
        abstract = attrs.pop('abstract',False)
        new_class = super_new(cls, name, bases, attrs)
        if not abstract:
            self = new_class(new_class.script, new_class.__name__)
            _scripts[self.name] = self
        return new_class
    
    
class RedisScript(RedisScriptMeta('_RS', (object,), {'abstract':True})):
    '''Class which helps the sending and receiving lua scripts.
    
    It uses the ``evalsha`` command.
    
    .. attribute:: script
    
        The lua script to run
        
    .. attribute:: required_scripts
    
        A list/tuple of other :class:`RedisScript` names required by this script
        to properly execute.
        
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
        return client.execute_command('EVALSHA', self.sha1, numkeys, *keys_args,
                                      **options)
       
    
################################################################################
##    BATTERY INCLUDED REDIS SCRIPTS
################################################################################
        
class countpattern(RedisScript):
    script = '''\
return # redis.call('keys', ARGV[1])
'''
    def preprocess_args(self, client, args):
        if args and client.prefix:
            args = tuple(('%s%s' % (client.prefix, a) for a in args))
        return args
    
# Delete all keys from a pattern and return the total number of keys deleted
# This fails when there are too many keys
_delpattern = '''\
local keys = redis.call('keys', KEYS[1])
if keys then
  return redis.call('del',unpack(keys))
else
  return 0
end
'''
# This just works
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
