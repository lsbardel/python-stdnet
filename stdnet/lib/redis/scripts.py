import os
from hashlib import sha1
from functools import partial

from stdnet.utils import zip
from .connection import RedisRequest
from .exceptions import NoScriptError, ScriptError
from .redisinfo import RedisKey


__all__ = ['RedisScript',
           'pairs_to_dict',
           'get_script',
           'registered_scripts',
           'script_command_callback',
           'read_lua_file']


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


def read_lua_file(dotted_module, path = None):
    '''Load lua script from the stdnet/lib/lua directory'''
    path = path or DEFAULT_LUA_PATH
    bits = dotted_module.split('.')
    bits[-1] += '.lua'
    name = os.path.join(path, *bits)
    with open(name) as f:
        return f.read()
    
 
class RedisScriptMeta(type):
    
    def __new__(cls, name, bases, attrs):
        super_new = super(RedisScriptMeta, cls).__new__
        abstract = attrs.pop('abstract',False)
        new_class = super_new(cls, name, bases, attrs)
        if not abstract:
            instance = new_class()
            script = instance.script
            if isinstance(script,(list,tuple)):
                script = '\n'.join(script)
                instance.script = script
            instance.sha1 = sha1(instance.script.encode('utf-8')).hexdigest()
            _scripts[new_class.__name__] = instance
        return new_class
    
RedisScriptBase = RedisScriptMeta('RedisScriptBase',(object,),{'abstract':True})


class RedisScript(RedisScriptBase):
    ''':class:`RedisScript` is a class which helps the sending and receiving
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
        
    def __repr__(self):
        return self.__class__.__name__
    __str__ = __repr__
    
    @property
    def name(self):
        return self.__class__.__name__
    
    def call(self, client, command, body, keys, *args, **options):
        options['script_name'] = str(self)
        args, options = self.preprocess_args(client, args, options)
        return client._eval(command, body, keys, *args, **options)
        
    def preprocess_args(self, client, args, options):
        '''A chance to modify the arguments before sending request to server'''
        return args, options
    
    def eval(self, client, keys, *args, **options):
        return self.call(client, 'EVAL', self.script, keys, *args, **options)
    
    def evalsha(self, client, keys, *args, **options):
        return self.call(client, 'EVALSHA', self.sha1, keys, *args, **options)
    
    def load(self, client, keys, *args, **options):
        '''Load this :class:`RedisScript` to redis and runs it using evalsha.
It returns the result of the `evalsha` command.'''
        client.script_load(self.script, script_name=self.name)
        return self.evalsha(client, keys, *args, **options)
        
    def start_callback(self, request, response, args, **options):
        if str(response) == NoScriptError.msg:
            response = NoScriptError()
            client = request.client
            if not client.pipelined:
                num_keys = args[1]
                keys, args = args[2:2+num_keys],args[2+num_keys:]
                pipe = client.pipeline()
                self.load(pipe, keys, *args, **options)
                result = pipe.execute()
                if isinstance(result, RedisRequest):
                    return result.add_callback(
                        lambda r : self.load_callback(request,r,args,**options))
                else:
                    return self.load_callback(request, result, args, **options)
            else:
                return response
        elif isinstance(response, Exception):
            raise ScriptError('EVALSHA', self, response)
        else:
            return self.callback(request, response, args, **options)
        
    def load_callback(self, request, result, args, **options):
        if isinstance(result[0], Exception):
            raise result[0]
            #raise ScriptError('Lua redis script "{0}" error. {1}'\
            #                          .format(self,result[1]))
        return result[1]
    
    def callback(self, request, response, args, **options):
        '''This is the only method user should override when writing a new
:class:`RedisScript`. By default it returns *response*.

:parameter request: a class:`RedisRequest`.
:parameter response: the parsed response from the remote redis server.
:parameter args: parameters of the redis script.
:parameter options: Additional options for the callback.
'''
        return response
    

def load_missing_scripts(pipe, commands, results):
    '''Load missing scripts in a pipeline. This function loops through the
*results* list and if one or more values are instances of
:class:`NoScriptError`, it loads the scripts and perform a new evaluation.
Commands which have *option* ``script_dependency`` set to the name
of a missing script, are also re-executed.'''
    toload = False
    for r in results:
        if isinstance(r, NoScriptError):
            toload = True
            break
    if not toload:
        return results
    loaded = set()
    positions = []
    for i, result in enumerate(zip(commands, results)):
        command, result = result    
        if isinstance(result, NoScriptError):
            name = command.options.get('script_name')
            if name:
                script = get_script(name)
                if script:
                    args = command.args
                    s = 2 # Starts from 2 as the first argument is the command
                    num_keys = args[s-1]
                    keys, args = args[s:s+num_keys], args[s+num_keys:]
                    if script.name not in loaded:
                        positions.append(-1)
                        loaded.add(script.name)
                        script.load(pipe, keys, *args, **command.options)
                    else:
                        script.evalsha(pipe, keys, *args, **command.options)
                    positions.append(i)
                    for c in command.callbacks:
                        pipe.add_callback(c)
        else:
            sc = command.options.get('script_dependency')
            if sc:
                if not isinstance(sc,(list,tuple)):
                    sc = (sc,)
                for s in sc:
                    if s in loaded:
                        pipe.command_stack.append(commands[i])
                        positions.append(i)
                        break
                
    res = pipe.execute()
    if isinstance(res,RedisRequest):
        return res.add_callback(partial(_load_missing_scripts,
                                        results, positions))
    else:
        return _load_missing_scripts(results, positions, res)
        

def _load_missing_scripts(results, positions, res):
    for i,r in zip(positions,res):
        if i == -1:
            if isinstance(r, Exception):
                raise r
            else:
                continue
        results[i] = r
    return results
    
################################################################################
##    BATTERY INCLUDED REDIS SCRIPTS
################################################################################
        
class countpattern(RedisScript):
    script = '''\
return # redis.call('keys', ARGV[1])
'''
    def preprocess_args(self, client, args, options):
        if args and client.prefix:
            args = ['%s%s' % (client.prefix, a) for a in args]
        return args, options
    
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
