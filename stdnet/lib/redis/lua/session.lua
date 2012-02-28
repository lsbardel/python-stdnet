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

-- Add or remove indices for an instance.
-- Return nothing if the update was succesful otherwise it returns the
-- error message (constaints were violated)
function update_indices(s, score, bk, id, idkey, indices, uniques, add)
    errors = {}
    for i,name in pairs(indices) do
        value = redis.call('hget', idkey, name)
        if uniques[i] == '1' then
            idxkey = bk .. ':uni:' .. name
            if add then
                if redis.call('hsetnx', idxkey, value, id) + 0 == 0 then
                    -- remove field `name` from the instance hashtable so that
                    -- the next call to update_indices won't delete the index
                    redis.call('hdel', idkey, name)
                    table.insert(errors, 'Unique constraint "' .. name .. '" violated.')
                end
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
    return errors
end

-- LOOP OVER INSTANCES TO ADD/CHANGE
results = {}
local i = 0
local bk = KEYS[1]
local s = ARGV[i+1] -- 's' for sorted sets, 'z' for zsets
local num_instances = ARGV[i+2] + 0
local length_indices = ARGV[i+3] + 0
local idx1 = i+3
i = idx1 + 2*length_indices
local indices = table_slice(ARGV,idx1+1,idx1+length_indices)
local uniques = table_slice(ARGV,idx1+length_indices+1,i)
local idset = bk .. ':id'
local j = 0
local result = {}

while j < num_instances do
    local action = ARGV[i+1]
    local id = ARGV[i+2]
    local score = ARGV[i+3]
    local idx0 = i+4
    local length_data = ARGV[idx0] + 0
    local data = table_slice(ARGV,idx0+1,idx0+length_data)
    local created_id = false
    i = idx0 + length_data

    -- ID NOT AVAILABLE. CREATE ONE
    if id == '' then
        created_id = true
        id = redis.call('incr', bk .. ':ids')
    end
    local idkey = bk .. ':obj:' .. id
    local original_values = {}
    if action == 'o' or action == 'c' then  -- override or change
        original_values = redis.call('hgetall', idkey)
        update_indices(s, score, bk, id, idkey, indices, uniques, false)
        if action == 'o' then
            redis.call('del', idkey)
        end
    end
    if s == 's' then
        redis.call('sadd', idset, id)
    else
        redis.call('zadd', idset, score, id)
    end
    if length_data > 0 then
        redis.call('hmset', idkey, unpack(data))
    end
    j = j + 1
    local error = update_indices(s, score, bk, id, idkey, indices, uniques, true)
    -- An error has occured. Rollback changes.
    if # error > 0 then
        -- Remove indices
        error = error[1]
        update_indices(s, score, bk, id, idkey, indices, uniques, false)
        if action == 'a' then
            redis.call('del', idkey)
            redis.call(s .. 'rem', idset, id)
            if created_id then
                redis.call('decr', bk .. ':ids')
                id = ''
            end
        elseif # original_values > 0 then
            redis.call('hmset', idkey, unpack(original_values))
            update_indices(s, score, bk, id, idkey, indices, uniques, true)
        end
        result[j] = {id, error}
    else
        result[j] = id
    end
end

return result
