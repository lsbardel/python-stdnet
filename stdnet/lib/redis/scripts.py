import os
from hashlib import sha1
from functools import partial

from stdnet.utils import zip
from .redisinfo import RedisKey

try:
    from .async import AsyncConnectionPool, is_async, 
except ImportError:
    AsyncConnectionPool = None
    def is_async(result):
        return False

__all__ = ['RedisScript',
           'pairs_to_dict',
           'get_script',
           'registered_scripts',
           'script_command_callback',
           'read_lua_file',
           'AsyncConnectionPool',
           'is_async']


p = os.path
DEFAULT_LUA_PATH = p.join(p.dirname(p.dirname(p.abspath(__file__))),'lua')


def script_command_callback(request, response, args, command=None,
                            script_name=None, **options):
    if isinstance(response, Exception):
        if script_name:
            command = ' ' + command if command else ''
            response = ScriptError('SCRIPT'+command, script_name, response)
        return response
    elif command in ('FLUSH', 'KILL'):
        return response == b'OK'
    elif command == 'LOAD':
        return response.decode(request.client.encoding)
    else:
        return [int(r) for r in response]
    

def eval_command_callback(request, response, args, script_name=None, **options):
    s = _scripts.get(script_name)
    if not s:
        return response
    else:
        return s.start_callback(request, response, args, **options)
    

def pairs_to_dict(response, encoding, value_encoder=0):
    "Create a dict given a list of key/value pairs"
    if response:
        v1 = (r.decode(encoding) for r in response[::2])
        v2 = response[1::2]
        if value_encoder:
            v2 = (value_encoder(v) for v in v2)
        return zip(v1,v2)
    else:
        return ()


_scripts = {}


def registered_scripts():
    return tuple(_scripts)

 
def get_script(script):
    return _scripts.get(script)


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
    
    def __call__(self, client, keys, *args):
        loaded = client.connection_pool.loaded_scripts
        to_load = loaded.difference(self.required_scripts)
        if to_load:
            # We need to make sure the script is loaded
            async_results = []
            for name in to_load:
                s = get_script(name)
                result = client.script_load(s.script)
                if is_async(result):
                    results.append(result)
            if async_results:
                # result may be asynchronous if it was executed right-away
                # by an asynchronous connection. In this case we add a callback
                return result.add_callback(
                        partial(self._call, client, keys, args))
        return self._call(client, keys, args)
    
    def preprocess_args(self, client, args):
        return args
        
    def callback(self, request, response, args, **options):
        '''This is the only method user should override when writing a new
:class:`RedisScript`. By default it returns *response*.

:parameter request: a class:`RedisRequest`.
:parameter response: the parsed response from the remote redis server.
:parameter args: parameters of the redis script.
:parameter options: Additional options for the callback.
'''
        return response
    
    def _call(self, client, keys, args, result=None):
        args = self.preprocess_args(client, args)
        client.connection_pool.loaded_scripts.add(self.name)
        numkeys = len(keys)
        keys_args = keys + args 
        res = client.execute_command('EVALSHA', self.sha1, numkeys, *keys_args)
       
    
################################################################################
##    BATTERY INCLUDED REDIS SCRIPTS
################################################################################
        
class countpattern(RedisScript):
    script = '''\
return # redis.call('keys', ARGV[1])
'''
    def preprocess_args(self, client, args):
        if args and client.prefix:
            args = ['%s%s' % (client.prefix, a) for a in args]
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
    
    def callback(self, request, response, args, **options):
        if not response or not options['withscores']:
            return response
        return zip(response[::2], map(float, response[1::2]))
    

class zdiffstore(RedisScript):
    script = read_lua_file('commands.zdiffstore')
    
    
class keyinfo(countpattern):
    script = read_lua_file('commands.keyinfo')
    
    def preprocess_args(self, client, args):
        if args and client.prefix:
            a = ['%s%s' % (client.prefix, args[0])]
            a.extend(args[1:])
            args = tuple(a)
        return args
    
    def callback(self, request, response, args, **options):
        client = request.client
        if client.pipelined:
            client = client.client
        encoding = request.encoding
        for key, typ, length, ttl, enc, idle in response:
            key = key.decode(encoding)[len(client.prefix):]
            yield RedisKey(id=key, client=client,
                           type=typ.decode(encoding),
                           length=length,
                           ttl=ttl if ttl != -1 else False,
                           encoding=enc.decode(encoding),
                           idle=idle)


class move2set(RedisScript):
    script = (read_lua_file('commands.utils'),
              read_lua_file('commands.move2set'))
