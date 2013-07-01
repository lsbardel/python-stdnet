import os
from hashlib import sha1
from functools import partial
from collections import namedtuple
from itertools import starmap

from stdnet.utils import zip, map, ispy3k
from stdnet.utils.dispatch import Signal

__all__ = ['RedisScript',
           'get_script',
           'registered_scripts',
           'read_lua_file',
           'redis_before_send',
           'redis_after_receive',
           'HAS_C_EXTENSIONS']

HAS_C_EXTENSIONS = False

try:
    from . import cparser
    HAS_C_EXTENSIONS = True
except ImportError:
    from . import parser as cparser


p = os.path
DEFAULT_LUA_PATH = p.join(p.dirname(p.abspath(__file__)), 'lua')

redis_connection = namedtuple('redis_connection', 'address db') 

###########################################################
#    REDIS EXECUTION SIGNALS
redis_before_send = Signal()
redis_after_receive = Signal()
###########################################################

###########################################################
#    GLOBAL REGISTERED SCRIPT DICTIONARY
_scripts = {}

def registered_scripts():
    return tuple(_scripts)

def get_script(script):
    return _scripts.get(script)
###########################################################

class RedisManagerBase(object):
    all_loaded_scripts = {}
    
    @property
    def loaded_scripts(self):
        if self.address not in self.all_loaded_scripts:
            self.all_loaded_scripts[self.address] = set()
        return self.__class__.all_loaded_scripts[self.address]
    
    def execute_script(self, client, to_load, callback):
        for name in to_load:
            s = get_script(name)
            client.script_load(s.script)
        return callback()
    
    def clear_scripts(self):
        self.all_loaded_scripts[self.address] = set()
        
    def _setup(self, address, db, parser):
        self.connection = redis_connection(address, int(db))
        self.redis_parser = parser
    
    @property
    def address(self):
        return self.connection.address
    
    @property
    def db(self):
        return self.connection.db
    
    if ispy3k:
        def decode_key(self, value):
            if isinstance(value, bytes):
                return value.decode(self.encoding, self.encoding_errors)
            else:
                return value
            
    else:   #pragma    nocover
        
        def decode_key(self, value):
            return value
    
    
class RedisManager(RedisManagerBase):
    
    if ispy3k:
        def encode(self, value):
            if isinstance(value, bytes):
                return value
            elif isinstance(value, float):
                value = repr(value)
            else:
                value = str(value)
            return value.encode(self.encoding, self.encoding_errors)
            
    else:   #pragma    nocover
        def encode(self, value):
            if isinstance(value, unicode):
                return value.encode(self.encoding, self.encoding_errors)
            elif isinstance(value, float):
                return repr(value)
            else:
                return str(value)
            
    def __pack_gen(self, args):
        e = self.encode
        crlf = b'\r\n'
        yield e('*%s\r\n'%len(args))
        for value in map(e, args):
            yield e('$%s\r\n' % len(value))
            yield value
            yield crlf
    
    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        data = b''.join(self.__pack_gen(args))
        redis_before_send.send_robust(RedisManager, data=data, args=args)
        return data
    
    def pack_pipeline(self, commands):
        '''Internal function for packing pipeline commands into a
command byte to be send to redis.'''
        pack = lambda *args: b''.join(self.__pack_gen(args))
        data = b''.join(starmap(pack, (args for args, _ in commands)))
        redis_before_send.send_robust(RedisManager, data=data, args=commands)
        return data
    
    
if HAS_C_EXTENSIONS:
    class CppRedisManager(RedisManagerBase, cparser.RedisManager):
        
        def pack_command(self, *args):
            "Pack a series of arguments into a value Redis command"
            data = self._pack_command(*args)
            redis_before_send.send_robust(RedisManager, data=data, args=args)
            return data
        
        def pack_pipeline(self, commands):
            '''Internal function for packing pipeline commands into a
    command byte to be send to redis.'''
            pack = self._pack_command
            data = b''.join(starmap(pack, (args for args, _ in commands)))
            redis_before_send.send_robust(RedisManager, data=data, args=commands)
            return data
else:
    CppRedisManager = RedisManager
    

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
    ''':class:`RedisScriptBase` is a class which helps the sending and receiving
lua scripts to redis via the ``evalsha`` command.

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
    
    def __call__(self, client, keys, *args, **options):
        loaded = client.connection_pool.loaded_scripts
        to_load = self.required_scripts.difference(loaded)
        callback = partial(self._call, client, keys, args, options, to_load)
        return client.connection_pool.execute_script(client, to_load, callback)
    
    def preprocess_args(self, client, args):
        return args
        
    def callback(self, response, **options):
        '''This is the only method user should override when writing a new
:class:`RedisScript`. By default it returns ``response``.

:parameter response: the response obtained from the script execution.
:parameter options: Additional options for the callback.
'''
        return response
    
    def _call(self, client, keys, args, options, loaded, result=None):
        args = self.preprocess_args(client, args)
        client.connection_pool.loaded_scripts.update(loaded)
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
