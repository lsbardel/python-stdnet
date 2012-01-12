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

local rkey = KEYS[1]
local bk = KEYS[2]
local num_fields = KEYS[3] + 0
local io = 3
local fields = table_slice(KEYS, io+1, io+num_fields)
io = io + num_fields + 1
local ordering = KEYS[io]

-- Perform custom ordering if required
if ordering == 'explicit' then
	ids = redis.call('sort', rkey, unpack(table_slice(KEYS,io+1,-1)))
else
	local start = KEYS[io+1] + 0
	local stop = KEYS[io+2] + 0
	if ordering == 'DESC' then
		ids = redis.call('zrevrange', rkey, start, stop)
	elseif ordering == 'ASC' then
		ids = redis.call('zrange', rkey, start, stop)
	elseif start > 0 or stop ~= -1 then
		ids = redis.call('sort', rkey, 'LIMIT', start, stop, 'ALPHA')
	else
		ids = redis.call('smembers', rkey)
	end
end

-- loop over ids and gather the data if needed
if num_fields == 0 then
	result = {}
	for i,id in pairs(ids) do
		result[i] = {id, redis.call('hgetall', bk .. ':obj:' .. id)}
	end
	return result
elseif table.getn(fields) == 1 and fields[1] == 'id' then
	return ids
else
	result = {}
	for i,id in pairs(ids) do
		result[i] = {id, redis.call('hmget', bk .. ':obj:' .. id, unpack(fields))}
	end
	return result
end
