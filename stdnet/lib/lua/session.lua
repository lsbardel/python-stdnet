-- Script for committing a stdnet.orm.Session to redis
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
function update_indices(s, score, bk, id, idkey, indices, uniques, add)
    for i,name in pairs(indices) do
        value = redis.call('hget', idkey, name)
        if uniques[i] == '1' then
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
	            if s == 's' then
	                redis.call('sadd', idxkey, id)
	            else
	                redis.call('zadd', idxkey, score, id)
	            end
	        else
	            redis.call(s .. 'rem', idxkey, id)
	        end
	    end
    end
end

-- LOOP OVER INSTANCES TO ADD/CHANGE/DELETE
results = {}
local N = table.getn(KEYS)
local i = 0
local t = 1
while i < N  do
    local bk = KEYS[i+1]
    local s = KEYS[i+2] -- 's' for sorted sets, 'z' for zsets
    local num_instances = KEYS[i+3] + 0
    local length_indices = KEYS[i+4] + 0
    local idx1 = i+4
    i = idx1 + 2*length_indices
    local indices = table_slice(KEYS,idx1+1,idx1+length_indices)
    local uniques = table_slice(KEYS,idx1+length_indices+1,i)
    local idset = bk .. ':id'
    local j = 0
    local result = {}
    
    while j < num_instances do
        local action = KEYS[i+1]
        local id = KEYS[i+2]
        local score = KEYS[i+3]
        local idx0 = i+4
        local length_data = KEYS[idx0] + 0
        local data = table_slice(KEYS,idx0+1,idx0+length_data)
        i = idx0 + length_data
    
	    -- ID NOT AVAILABLE. CREATE ONE
	    if id == '' then
	        id = redis.call('incr',bk .. ':ids')
	    end
	    local idkey = bk .. ':obj:' .. id	  
        if action == 'c' then
            update_indices(s, score, bk, id, idkey, indices, uniques, false)
        end
        if s == 's' then
            redis.call('sadd', idset, id)
        else
            redis.call('zadd', idset, score, id)
        end
        if length_data > 0 then
           redis.call('hmset', idkey, unpack(data))
        end
        update_indices(s, score, bk, id, idkey, indices, uniques, true)
	    j = j + 1
	    result[j] = id
	end
	results[t] = result
	t = t + 1
end

return results