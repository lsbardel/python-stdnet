import os
from hashlib import sha1
from functools import partial

from stdnet.utils import zip
from .connection import RedisRequest
from .exceptions import NoScriptError
from .redisinfo import RedisKey


__all__ = ['RedisScript',
           'pairs_to_dict',
           'get_script',
           'registered_scripts',
           'read_lua_file']


def pairs_to_dict(response, encoding, value_encoder = 0):
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


def read_lua_file(filename, path = None):
    '''Load lua script from the stdnet/lib/lua directory'''
    if not path:
        path = os.path.split(os.path.abspath(__file__))[0]
        path = os.path.join(path,'lua')
    name = os.path.join(path,filename)
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
        
    def __str__(self):
        return self.__class__.__name__
    __repr__ = __str__
    
    @property
    def name(self):
        return self.__class__.__name__
    
    def call(self, client, command, body, keys, *args, **options):
        options['script_name'] = str(self)
        return client._eval(command, body, keys, *args, **options)
        
    def eval(self, client, keys, *args, **options):
        return self.call(client, 'EVAL', self.script, keys, *args, **options)
    
    def evalsha(self, client, keys, *args, **options):
        return self.call(client, 'EVALSHA', self.sha1, keys, *args, **options)
    
    def load(self, client, keys, *args, **options):
        client.script_load(self.script)
        return self.evalsha(client, keys, *args, **options)
        
    def start_callback(self, request, response, args, **options):
        if isinstance(response,NoScriptError):
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
        else:
            return self.callback(request, response, args, **options)
        
    def load_callback(self, request, result, args, **options):
        if isinstance(result[0],Exception):
            raise result[0]
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
    
    
def script_call_back(request, response, args, script_name = None, **options):
    s = _scripts.get(script_name)
    if not s:
        return response
    else:
        return s.start_callback(request, response, args, **options)
    

def load_missing_scripts(pipe, commands, results):
    '''Load missing scripts in a pipeline. This function loops thorugh the
*results* list and if one or more values are instances of
:class:`NoScriptError`, it loads the scripts and perfrom a new evaluation.
Commands which have *option* ``script_dependency`` set to the name
of a missing script, are also re-executed.'''
    toload = False
    for r in results:
        if isinstance(r,NoScriptError):
            toload = True
            break
    if not toload:
        return results
    
    loaded = set()
    positions = []
    for i,result in enumerate(zip(commands,results)):
        args, options, callbacks = result[0]    
        if isinstance(result[1],NoScriptError):
            name = options.get('script_name')
            if name:
                script = get_script(name)
                if script:
                    s = 3 # Starts from 3 as the first argument is the command
                    num_keys = args[s-1]
                    keys, args = args[s:s+num_keys],args[s+num_keys:]
                    if script.name not in loaded:
                        positions.append(-1)
                        loaded.add(script.name)
                        script.load(pipe, keys, *args, **options)
                    else:
                        script.evalsha(pipe, keys, *args, **options)
                    positions.append(i)
                    for c in callbacks:
                        pipe.add_callback(c)
        else:
            sc = options.get('script_dependency')
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
            if isinstance(r,Exception):
                raise r
            else:
                continue
        results[i] = r
    return results
    
                
class ScriptCommand(object):
    __slots__ = ('builder','name')
    
    def __init__(self, builder, name):
        self.builder = builder
        self.name = name
        
    def __repr__(self):
        return self.name
    __str__ = __repr__
    
    def __call__(self, key, *args):
        sargs = ','.join(("'"+a+"'" if isinstance(a,str)\
                           else str(a) for a in args))
        name = "res = redis.call('"+self.name+"','"+key+"'"
        if sargs:
            name += ',' + sargs
        command = name + ')'
        self.builder.append(command)
        return self.builder
        
        
class countpattern(RedisScript):
    script = '''\
return table.getn(redis.call('keys',KEYS[1]))
'''
    
# Delete all keys from a pattern and return the total number of keys deleted
# This fails when there are too many keys
_delpattern = '''\
keys = redis.call('keys',KEYS[1])
if keys then
  return redis.call('del',unpack(keys))
else
  return 0
end
'''
# This just works
class delpattern(RedisScript):
    script = '''\
n = 0
for i,key in ipairs(redis.call('keys',KEYS[1])) do
  n = n + redis.call('del',key)
end
return n
'''

class zpop(RedisScript):
    script = read_lua_file('zpop.lua')
    
    def callback(self, request, response, args, **options):
        if not response or not options['withscores']:
            return response
        return zip(response[::2], map(float, response[1::2]))
    

class zdiffstore(RedisScript):
    script = read_lua_file('zdiffstore.lua')
    
    
    
class keyinfo(RedisScript):
    script = read_lua_file('keyinfo.lua')
    
    def callback(self, request, response, args, **options):
        client = request.client
        encoding = request.encoding
        for key,typ,len,ttl,enc,idle in response:
            yield RedisKey(id = key.decode(encoding),\
                           client = client,
                           type = typ.decode(encoding),\
                           length = len,
                           ttl = ttl if ttl != -1 else False,\
                           encoding = enc.decode(encoding),\
                           idle = idle)
