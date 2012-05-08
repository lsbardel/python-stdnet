-- Script for committing a stdnet.orm.Session to redis
local results = {}
local i = 0
local bk = KEYS[1]
local s = ARGV[i+1] -- 's' for sorted sets, 'z' for zsets
local num_instances = ARGV[i+2] + 0
local length_indices = ARGV[i+3] + 0
local id_info = ARGV[i+4]
local idx1 = i+4
local composite_id = false
local auto_id = false
if id_info == 'auto' then
    auto_id = true
elseif id_info ~= '' then
    id_info = id_info + 0
    composite_id = tabletools.slice(ARGV,idx1+1,idx1+id_info)
    idx1 = idx1 + id_info
end 
i = idx1 + 2*length_indices
local indices = tabletools.slice(ARGV,idx1+1,idx1+length_indices)
local uniques = tabletools.slice(ARGV,idx1+length_indices+1,i)
local idset = bk .. ':id'
local j = 0
local result = {}

-- Add or remove indices for an instance.
-- Return nothing if the update was succesful otherwise it returns the
-- error message (constaints were violated)
local function update_indices(score, id, idkey, oldid, add)
    local errors = {}
    local idxkey
    for i,name in pairs(indices) do
        local value = redis.call('hget', idkey, name)
        if uniques[i] == '1' then
            idxkey = bk .. ':uni:' .. name
            if add then
                if redis.call('hsetnx', idxkey, value, id) + 0 == 0 then
                    if oldid == id or not redis.call('hget', idxkey, value) == oldid then
	                    -- remove field `name` from the instance hashtable so that
	                    -- the next call to update_indices won't delete the index
	                    redis.call('hdel', idkey, name)
	                    table.insert(errors, 'Unique constraint "' .. name .. '" violated.')
	                end
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

-- Update a composite ID. Composite IDs are formed by two or more fields
-- in an unique way.
local function update_composite_id(original_values, data)
    local fields = {}
    local j = 0
    while j < # original_values do
        fields[original_values[j+1]] = original_values[j+2]
        j = j + 2
    end
    j = 0
    while j < # data do
        fields[data[j+1]] = data[j+2]
        j = j + 2
    end
    local newid = ''
    local joiner = ''
    for _,name in ipairs(composite_id) do
        newid = newid .. joiner .. name .. ':' .. fields[name]
        joiner = ','
    end
    return newid
end

-- LOOP OVER INSTANCES TO ADD/CHANGE
while j < num_instances do
    local action = ARGV[i+1]
    local id = ARGV[i+2]
    local score = ARGV[i+3]
    local idx0 = i+4
    local length_data = ARGV[idx0] + 0
    local data = tabletools.slice(ARGV,idx0+1,idx0+length_data)
    local created_id = false
    local autoincr = false
    local errors = {}
    if score:find(' ') == 5 then
        score = score:sub(6) + 0
        autoincr = true
    end
    j = j + 1
    i = idx0 + length_data

    -- AUTO ID
    if auto_id then
        if id == '' then
            created_id = true
            id = redis.call('incr', bk .. ':ids')
        else
        	id = id + 0	 --	must be numeric
        	local counter = redis.call('get', bk .. ':ids')
        	if not counter or counter + 0 < id then
                redis.call('set', bk .. ':ids', id)
            end
        end
    end
    
    if id == '' and not composite_id then
        table.insert(errors, 'Id not avaiable.')
    end
    
    if # errors == 0 then
        local oldid = id
	    local idkey = bk .. ':obj:' .. oldid
	    local original_values = {}
	    if action == 'o' or action == 'c' then  -- override or change
	        original_values = redis.call('hgetall', idkey)
	        update_indices(score, id, idkey, oldid, false)
	        if action == 'o' then
	            redis.call('del', idkey)
	        end
	    end
	    -- Composite ID. Calculate new ID and data
	    if composite_id then
	        id = update_composite_id(original_values, data)
	    end
	    idkey = bk .. ':obj:' .. id
	    if id ~= oldid and oldid ~= '' then
            if s == 's' then
                redis.call('srem', idset, oldid)
            else
                redis.call('zrem', idset, oldid)
            end
        end
	    if s == 's' then
	        redis.call('sadd', idset, id)
	    elseif autoincr then   --  Autoincrement score if id is repeated
	        score = redis.call('zincrby', idset, score, id)
	    else
	        redis.call('zadd', idset, score, id)
	    end
	    if length_data > 0 then
	        redis.call('hmset', idkey, unpack(data))
	    end
	    errors = update_indices(score, id, idkey, oldid, true)
	    -- An error has occured. Rollback changes.
	    if # errors > 0 then
	        -- Remove indices
	        update_indices(score, id, idkey, oldid, false)
	        if action == 'a' then
	            redis.call('del', idkey)
	            redis.call(s .. 'rem', idset, id)
	            if created_id then
	                redis.call('decr', bk .. ':ids')
	                id = ''
	            end
	        elseif # original_values > 0 then
	            id = oldid
                idkey = bk .. ':obj:' .. id
	            redis.call('hmset', idkey, unpack(original_values))
	            update_indices(score, id, idkey, oldid, true)
	        end
	    end
	end
	if # errors > 0 then
        result[j] = {id, 0, errors[1]}
    else
        result[j] = {id, 1, score}
    end
end

return result
