-- Retrieve information about keys
local key
if # ARGV > 0 then
    keys = redis.call('KEYS',ARGV[1])
elseif # KEYS > 0 then
    keys = KEYS
else
    return {}
end
local type_table = {}
type_table['set'] = 'scard'
type_table['zset'] = 'zcard'
type_table['list'] = 'llen'
type_table['hash'] = 'hlen'
type_table['string'] = 'strlen'
local stats = {}
local typ, command, len
for i,key in ipairs(keys) do
    idletime = redis.call('object','idletime',key)
    typ = redis.call('type',key)['ok']
    command = type_table[typ]
    len = 0
    if command then
        len = len + redis.call(command, key)
    end
    stats[i] = {key,typ,len,redis.call('ttl',key),
                redis.call('object','encoding',key),
                idletime}
end
return stats