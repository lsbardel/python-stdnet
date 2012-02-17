-- REDIS ZPOP command. you can pop one or more elements form the sorted set
local key = KEYS[1]
local start = KEYS[2]
local stop = KEYS[3]
local range = redis.call('ZRANGE', key, start, stop)
redis.call('ZREMRANGEBYRANK', key, start, stop)
return range