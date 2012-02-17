
class update_observer(redis.RedisScript):
    '''Acript for adding/updating an observer. The ARGV contains, the meber
value, the initial score (usually a timestamp) and the increment for
subsequent additions.'''
    script = '''\
local key = KEYS[1]
local member = ARGV[1]
if redis.call('zrank', id, member) then
    redis.call('zincrby', id, ARGV[3], member)
else
    redis.call('zadd', id, ARGV[2], member)
end
'''