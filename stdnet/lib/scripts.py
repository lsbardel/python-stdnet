import os

from .py2py3 import zip


__all__ = ['RedisScript','pairs_to_dict',
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
        