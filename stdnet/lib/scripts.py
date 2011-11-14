# This just works
countpattern = '''\
return table.getn(redis.call('keys',KEYS[1]))
'''
# Delete all keys from a pattern and return the total number of keys deleted
# This fails when there are too many keys
delpattern = '''\
keys = redis.call('keys',KEYS[1])
if keys then
  return redis.call('del',unpack(keys))
else
  return 0
end
'''
# This just works
delpattern = '''\
n = 0
for i,key in ipairs(redis.call('keys',KEYS[1])) do
  n = n + redis.call('del',key)
end
return n
'''