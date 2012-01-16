--DELETE A QUERY
function table_slice (values,i1,i2)
    local res = {}
    local n = #values
    -- default values for range
    i1 = i1 or 1
    i2 = i2 or n
    if i2 < 0 then
        i2 = n + i2 + 1
    elseif i2 > n then
        i2 = n
    end
    if i1 < 1 or i1 > n then
        return {}
    end
    local k = 1
    for i = i1,i2 do
        res[k] = values[i]
        k = k + 1
    end
    return res
end

-- Add or remove indices for an instance
function update_indices(s, bk, id, idkey, indices, uniques)
    for i,name in pairs(indices) do
        value = redis.call('hget', idkey, name)
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

bk = KEYS[1] -- base key for model
rkey = KEYS[2] -- the key where to store the structure containing the resuls
local s = KEYS[3]
local length_indices = KEYS[4]
local idx1 = i+4
local indices = table_slice(KEYS,idx1+1,idx1+length_indices)
local uniques = table_slice(KEYS,idx1+length_indices+1,idx1+2*length_indices)
local idset = bk .. ':id'
local ty = redis.call('type',rkey)
ids = {}
a = 0
if ty == 'set' then
    a = 1
    ids = redis.call('smembers',rkey)
elseif ty == 'zset' then
    a = 2
    ids = redis.call('zmembers',rkey)
end

return {ty,a,ids}
