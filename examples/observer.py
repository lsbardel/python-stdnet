'''This example is an implementation of the Observer design-pattern
when Observers receives multiple updates from several instances they are
observing.
'''
from time import time
from stdnet import odm
from stdnet.odm import struct
from stdnet.backends import redisb

class update_observer(redisb.RedisScript):
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
            flat = tuple(self.flat(cache.toadd.items()))
            self.client.execute_script('update_observer', (self.id,), *flat)
            result = True
        if cache.toremove:
            flat = tuple((el[1] for el in cache.toremove))
            self.client.zrem(self.id, *flat)
            result = True
        return result
    
    def flat(self, zs):
        for s, el in zs:
            yield s
            yield el[1]
            yield el[2]


class UpdateZset(odm.Zset):
    penalty = 0 # penalty in seconds
    
    def __init__(self, *args, **kwargs):
        self.penalty = kwargs.pop('penalty',self.penalty)
        super(UpdateZset,self).__init__(*args, **kwargs)
        
    def dump_data(self, instances):
        dt = time()
        for n, instance in enumerate(instances):
            if hasattr(instance, 'pkvalue'):
                instance = instance.pkvalue()
            # put n so that it allows for repeated values
            yield dt, (n, self.penalty, instance)

# Register the new structure with redis backend
redisb.BackendDataServer.struct_map['updatezset'] = RedisUpdateZset

class UpdatesField(odm.StructureField):
    
    def structure_class(self):
        return UpdateZset
    
    
class Observable(odm.StdModel):
    pass


class Observer(odm.StdModel):
    # Underlyings are the Obsarvable this Observer is tracking for updates
    underlyings = odm.ManyToManyField(Observable, related_name='observers')
    
    # field with a 5 seconds penalty
    updates = UpdatesField(class_field=True, penalty=5)
    
    
def update_observers(sender, instances, session=None, **kwargs):
    # This callback must be registered with the router
    # post_commit method
    # Instances of observable got an update. Loop through the updated observables
    # and push to the observer class updates all the observers of the observable.
    models = session.router
    observers = models.observer
    through = models[observers.underlyings.model]
    all = yield through.filter(observable=instances).get_field('observer').all()
    if all:
        yield observers.updates.update(all)

            
    
    

    