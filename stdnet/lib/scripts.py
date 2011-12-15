# This just works

class nil(object):
    pass

_scripts = {}

def get_script(script):
    return _scripts.get(script)
 
 
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
    
    
def script_call_back(response, script = None, args = None, **options):
    s = _scripts.get(script)
    if not s:
        raise ValueError('No such script {0}'.format(script))
    return s.callback(response, args, **options)
    
        
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
        