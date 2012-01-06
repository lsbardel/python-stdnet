import os

from .py2py3 import zip


__all__ = ['RedisScript','ScriptBuilder','pairs_to_dict',
           'read_lua_file','nil']


class nil(object):
    pass


def pairs_to_dict(response, encoding = 'utf-8'):
    "Create a dict given a list of key/value pairs"
    if response:
        return zip((r.decode(encoding) for r in response[::2]), response[1::2])
    else:
        return ()

_scripts = {}


def get_script(script):
    return _scripts.get(script)
 
 
def read_lua_file(filename):
    '''Load lua script from the stdnet/lib/lua directory'''
    path = os.path.split(os.path.abspath(__file__))[0]
    name = os.path.join(path,'lua',filename)
    with open(name) as f:
        return f.read()
    
 
class RedisScriptMeta(type):
    
    def __new__(cls, name, bases, attrs):
        super_new = super(RedisScriptMeta, cls).__new__
        abstract = attrs.pop('abstract',False)
        new_class = super_new(cls, name, bases, attrs)
        if not abstract:
            _scripts[new_class.__name__] = new_class()
        return new_class
    
RedisScriptBase = RedisScriptMeta('RedisScriptBase',(object,),{'abstract':True})


class RedisScript(RedisScriptBase):
    abstract = True
    script = None
        
    def __str__(self):
        return self.__class__.__name__
    __repr__ = __str__
    
    def callback(self, response, args, **options):
        return response
    
    
def script_call_back(response, script_name = None, script_args = None,
                     **options):
    s = _scripts.get(script_name)
    if not s:
        return response
    else:
        return s.callback(response, script_args, **options)
    

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

class hash_pop_item(RedisScript):
    script = '''\
elem = redis.call('hget',KEYS[1],KEYS[2])
pop = redis.call('hdel',KEYS[1],KEYS[2])
return {pop,elem}
'''
    def callback(self, response, args, **options):
        if response[0] == 0:
            return nil
        return response[1]
        

class commit_session(RedisScript):
    script = read_lua_file('session.lua')
    
    def callback(self, response, args, session = None):
        data = []
        for state,id in zip(session,response):
            if not state.deleted:
                instance = state.instance
                instance.id = instance._meta.pk.to_python(id)
                data.append(instance)
        return data
    
