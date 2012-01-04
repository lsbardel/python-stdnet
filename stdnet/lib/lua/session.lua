-- Script for committing a stdnet session to redis

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

-- ADD OR REMOVE INDICES FOR INSTANCE
function update_indices(s, bk, id, idkey, indices, uniques, add)
    for i,name in pairs(indices) do
        value = redis.call('hget', idkey, name)
        if uniques[i] == 1 then
            idxkey = bk .. ':uni:' .. name
            if add then
                redis.call('hset', idxkey, value, id)
            else
                redis.call('hdel', idxkey, value)
            end
        else
        	idxkey = bk .. ':idx:' .. name .. ':'
        	if value then
	        	idxkey = idxkey .. value
	        end
	        if add then
	            redis.call(s .. 'add', idxkey, id)
	        else
	            redis.call(s .. 'rem', idxkey, id)
	        end
	    end
    end
end

-- LOOP OVER INSTANCES TO ADD/CHANGE/DELETE
result = {}
local N = table.getn(KEYS)
local i = 0
local t = 1
while i < N  do
    local action = KEYS[i+1]
    local bk = KEYS[i+2]
    local id = KEYS[i+3]
    local s = KEYS[i+4] -- 's' for sorted sets, 'z' for zsets
    local idx0 = i+5
    local length_data = KEYS[idx0]
    local idx1 = idx0+length_data+1
    local length_indices = KEYS[idx1]
    local indices = table_slice(KEYS,idx1+1,idx1+length_indices)
    local uniques = table_slice(KEYS,idx1+length_indices+1,idx1+2*length_indices)
    
    -- ID NOT AVAILABLE. CREATE ONE
    if id == '' then
        id = redis.call('incr',bk .. ':ids')
    end
    local idkey = bk .. ':obj:' .. id
  
    -- DELETING THE INSTANCE
    if action == 'del' then
        update_indices(s, bk, id, idkey, indices, uniques, false)
        redis.call('del', idkey)
        redis.call('srem', idkey)
    -- ADDING OR EDITING THE INSTANCE
    else
        if action == 'change' then
            update_indices(s, bk, id, idkey, indices, uniques, false)
        end
        local data = table_slice(KEYS,idx0+1,idx0+length_data)
        redis.call('sadd', bk .. ':id', id)
        redis.call('hmset', idkey, unpack(data))
        update_indices(s, bk, id, idkey, indices, uniques, true)
    end
    result[t] = id
    t = t + 1
    i = idx1 + 2*length_indices
end

return result