-- LOAD INSTANCES DATA FROM AN EXISTING QUERY
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

local rkey = KEYS[1]  -- Key containing the ids of the query
local bk = KEYS[2] -- Base key for model
local num_fields = KEYS[3] + 0
local io = 3
local fields = table_slice(KEYS, io+1, io+num_fields)
io = io + num_fields + 1
local ordering = KEYS[io]
local start = KEYS[io+1] + 0
local stop = KEYS[io+2] + 0
io = io + 2

function randomkey()
	rkey = bk .. ':tmp:' .. math.random(1,100000000)
	if redis.call('exists', rkey) + 0 == 1 then
		return randomkey()
	else
		return rkey
	end
end

function members(key)
	local typ = redis.call('type',key)['ok']
	if typ == 'set' then
		return redis.call('smembers',key)
	elseif typ == 'zset' then
		return redis.call('zrange',key,0,-1)
	elseif typ == 'list' then
		return redis.call('lrange',key,0,-1)
	else
		return {}
	end
end

-- Perform explicit custom ordering if required
if ordering == 'explicit' then
	local field = KEYS[io+1]
	local alpha = KEYS[io+2]
	local desc = KEYS[io+3]
	local nested = KEYS[io+4] + 0
	io = io + 4
	-- nested sorting for foreign key fields
	if nested > 0 then
		local skey = randomkey()
		local mids = members(rkey)
		for _,id in pairs(mids) do
			local value = redis.call('hget', bk .. ':obj:' .. id, field)
			local n = 0
			while n < nested do
				ion = io + 2*n
				n = n + 1
				key = KEYS[ion+1] .. ':obj:' .. value
				name = KEYS[ion+2]
				value = redis.call('hget', key, name)
			end
			-- store value on temporary hash table
			redis.call('hset', skey, id, value)
		end
		bykey = skey .. '->*'
		redis.call('expire', skey, 5)
		-- return mids
	else
		bykey = bk .. ':obj:*->' .. field
	end
	local sortargs = {'BY',bykey}
	if start > 0 or stop ~= -1 then
		table.insert(sortargs,'LIMIT')
		table.insert(sortargs,start)
		table.insert(sortargs,stop)
	end
	if alpha == 'ALPHA' then
		table.insert(sortargs,alpha)
	end
	if desc == 'DESC' then
		table.insert(sortargs,desc)
	end
	ids = redis.call('sort', rkey, unpack(sortargs))
else
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
