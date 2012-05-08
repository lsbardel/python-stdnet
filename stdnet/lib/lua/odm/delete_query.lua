--DELETE A QUERY for a model

-- Add or remove indices for an instance
local function update_indices(s, bk, id, idkey, indices, uniques)
    local idxkey
    for i, name in pairs(indices) do
        local value = redis.call('hget', idkey, name)
        if uniques[i] == '1' then
            idxkey = bk .. ':uni:' .. name
            redis.call('hdel', idxkey, value)
        else
            idxkey = bk .. ':idx:' .. name .. ':'
            if value then
                idxkey = idxkey .. value
            end
            redis.call(s .. 'rem', idxkey, id)
        end
    end
end

local bk = KEYS[1] -- base key for model
local rkey = KEYS[2] -- the key where to store the structure containing the resuls
local s = ARGV[1]
local length_indices = ARGV[2]
local idx1 = 2
local indices = tabletools.slice(ARGV,idx1+1,idx1+length_indices)
local uniques = tabletools.slice(ARGV,idx1+length_indices+1,idx1+2*length_indices)
idx1 = idx1 + 2*length_indices + 1
local length_multifields = ARGV[idx1]
local multifields = tabletools.slice(ARGV, idx1+1, idx1+length_multifields)
--i = idx1 + length_multifields
local idset = bk .. ':id'
local ty = redis.call('type',rkey)['ok']
local ids
if ty == 'set' then
    ids = redis.call('smembers', rkey)
elseif ty == 'zset' then
    ids = redis.call('zrange', rkey, 0, -1)
elseif ty == 'list' then
    ids = redis.call('lrange', rkey, 0, -1)
else
	ids = {}
end

local j = 0
local results = {}
-- Loop over query ids and remove all keys associated with instances and update indices
for _,id in ipairs(ids) do
    local idkey = bk .. ':obj:' .. id
    update_indices(s, bk, id, idkey, indices, uniques)
    local num = redis.call('del', idkey) + 0
    redis.call(s .. 'rem', idset, id)
    for _,name in ipairs(multifields) do
        redis.call('del', idkey .. ':' .. name)
    end 
    if num == 1 then
    	j = j + 1
        results[j] = id
    end
end

return results