import os
from hashlib import sha1
from functools import partial

from stdnet.utils import zip
from .connection import RedisRequest
from .exceptions import NoScriptError


__all__ = ['RedisScript',
           'ScriptBuilder',
           'pairs_to_dict',
           'get_script',
           'read_lua_file',
           'nil']


class nil(object):
    pass


def pairs_to_dict(request, response, args, **options):
    "Create a dict given a list of key/value pairs"
    if response:
        encoding = request.client.encoding
        return zip((r.decode(encoding) for r in response[::2]), response[1::2])
    else:
        return ()


_scripts = {}


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
lua scripts to redis via the ``eval`` or ``evalsha`` command.'''
    abstract = True
    script = None
        
    def __str__(self):
        return self.__class__.__name__
    __repr__ = __str__
    
    def call(self, client, command, script, *args, **options):
        options['script_name'] = str(self)
        return client.execute_command(command, script, len(args),
                                      *args, **options)
        
    def evalsha(self, client, *args, **options):
        return self.call(client, 'EVALSHA', self.sha1, *args, **options)
    
    def eval(self, client, *args, **options):
        return self.call(client, 'EVAL', self.script, *args, **options)
        
    def start_callback(self, request, response, args, **options):
        if isinstance(response,NoScriptError):
            client = request.client
            if not client.pipelined:
                pipe = client.pipeline()
                pipe.script_load(self.script)
                args = args[2:]
                self.eval(pipe, self.script, *args, **options)
                result = pipe.execute()
                if isinstance(result, RedisRequest):
                    return result.add_callback(
                                partial(self._start_callback, args, options))
                else:
                    return self._start_callback(request, args, options, result)
            else:
                return response
        else:
            return self.callback(request, response, args, **options)
    
    def _start_callback(self, request, args, options, response):
        #if response[0] == self.sha1:
        return self.callback(request, response[1], args, **options)
        
    def callback(self, request, response, args, **options):
        return response
    
    
def script_call_back(request, response, args, script_name = None, **options):
    if response == b'NOSCRIPT No matching script. Please use EVAL.':
        response = NoScriptError()
    s = _scripts.get(script_name)
    if not s:
        return response
    else:
        return s.start_callback(request, response, args, **options)
    

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
        
        
class ScriptBuilder(object):
    
    def __init__(self, redis):
        self.redis = redis
        self.script = None
        self.lines = []
    
    def __getattr__(self, name):
        return ScriptCommand(self, name)
    
    def append(self, line):
        if self.script:
            raise ValueError('Script has been executed already. Run clear')
        self.lines.append(line)
        
    def clear(self):
        self.script = None
        self.lines = []
        
    def execute(self):
        if self.lines:
            lines = self.lines
            lines.append('return res')
            self.script = '\n'.join(lines)
            return self.redis.eval(self.script)
        
        
countpattern = '''\
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
