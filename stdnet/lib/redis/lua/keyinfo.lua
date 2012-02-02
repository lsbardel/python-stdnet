local pattern = KEYS[1]
local type_table = {}
type_table['set'] = 'scard'
type_table['zset'] = 'zcard'
type_table['list'] = 'llen'
type_table['hash'] = 'hlen'
type_table['string'] = 'strlen'
local stats = {}
local typ, command, len
for i,key in ipairs(redis.call('KEYS',pattern)) do
    typ = redis.call('type',key)['ok']
    command = type_table[typ]
    len = 0
    if command then
        len = len + redis.call(command, key)
    end
    stats[i] = {key,typ,len,redis.call('ttl',key),
                redis.call('object','encoding',key),
                redis.call('object','idletime',key)}
end
return stats