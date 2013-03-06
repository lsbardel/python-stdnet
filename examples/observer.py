'''This example is an implementation of the Observer design-pattern
when Observers receives multiple updates from several instances they are
observing.
'''
from time import time
from stdnet import odm
from stdnet.odm import struct
from stdnet.lib import redis
from stdnet.backends import redisb

class update_observer(redis.RedisScript):
    '''Script for adding/updating an observer. The ARGV contains, the member
value, the initial score (usually a timestamp) and the increment for
subsequent additions.'''
    script = '''\
local key = KEYS[1]
local index = 0
local n = 0
while index < # ARGV do
    local score = ARGV[index+1]+0
    local penalty = ARGV[index+2]+0
    local member = ARGV[index+3]
    index = index + 3
    if redis.call('zrank', key, member) then
        redis.call('zincrby', key, -penalty, member)
    else
        n = n + redis.call('zadd', key, score, member)
    end
end
return n
'''     

class RedisUpdateZset(redisb.Zset):
    '''Redis backend structure override Zset'''
    def flush(self):
        cache = self.instance.cache
        result = None
        if cache.toadd:
            flat = tuple(self.flat(cache.toadd))
            self.client.execute_script('update_observer', self.id, *flat)
            result = True
        if cache.toremove:
            flat = tuple((el[1] for el in cache.toremove))
            self.client.zrem(self.id, *flat)
            result = True
        return result
    
    def flat(self, zs):
        for s,el in zs:
            yield s
            yield el[0]
            yield el[1]


class UpdateZset(odm.Zset):
    penalty = 0 # penalty in seconds
    
    def __init__(self, *args, **kwargs):
        self.penalty = kwargs.pop('penalty',self.penalty)
        super(UpdateZset,self).__init__(*args, **kwargs)
    
    def add(self, instance):
        self.update((instance,))
        
    def dump_data(self, instances):
        for instance in instances:
            yield time(),(self.penalty,instance.id)

# Register the new structure with redis backend
redisb.BackendDataServer.struct_map['updatezset'] = RedisUpdateZset

class UpdatesField(odm.StructureField):
    
    def structure_class(self):
        return UpdateZset
    
    
class Observable(odm.StdModel):
    pass


class Observer(odm.StdModel):
    underlyings = odm.ManyToManyField(Observable, related_name = 'observers')
    
    # field with a 5 seconds penalty
    updates = UpdatesField(class_field = True,
                           penalty = 5)
    
    
def update_observers(sender, instances, **kwargs):
    for observable in instances:
        Observer.updates.update(observable.observers.query())

# Register event
odm.post_commit.connect(update_observers, sender = Observable)
            
    
    

    