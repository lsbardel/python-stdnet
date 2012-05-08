-- LOAD INSTANCES DATA FROM AN EXISTING QUERY
local function get_related_fields (args, i, num)
	local all = {}
	local count = 0
	while count < num do
		local related = {bk = args[i+1], name = args[i+2], field = args[i+3], type = args[i+4]}
		i = i + 5
		local nf = args[i] + 0
		related['fields'] = tabletools.slice(args, i+1, i+nf)
		i = i + nf
		count = count + 1
		all[count] = related
	end
	return {all, i}
end

-- Handle input arguments
local rkey = KEYS[1]  -- Key containing the ids of the query
local bk = KEYS[2] -- Base key for model
local get_field = ARGV[1]
local ids
local result
local io = 2
local num_fields = ARGV[io] + 0
local fields = tabletools.slice(ARGV, io + 1, io + num_fields)
local related
io = io + num_fields + 1
related, io = unpack(get_related_fields(ARGV, io, ARGV[io] + 0))
local ordering = ARGV[io+1]
local start = ARGV[io+2] + 0
local stop = ARGV[io+3] + 0
io = io + 3

if get_field ~= '' then
	return redis_members(rkey)
end

-- Perform explicit custom ordering if required
if ordering == 'explicit' then
	local field = ARGV[io+1]
	local alpha = ARGV[io+2]
	local desc = ARGV[io+3]
	local nested = ARGV[io+4] + 0
	local tkeys = {}
	local sortargs = {}
	local bykey
	io = io + 4
    -- nested sorting for foreign key fields
	if nested > 0 then
		-- generate a temporary key where to store the hash table holding
		-- the values to sort with
		local ion, key, name
		local skey = redis_randomkey(bk)
		for i,id in pairs(redis_members(rkey)) do
			local value = redis.call('hget', bk .. ':obj:' .. id, field)
			local n = 0
			while n < nested do
				ion = io + 2*n
				n = n + 1
				key = ARGV[ion+1] .. ':obj:' .. value
				name = ARGV[ion+2]
				value = redis.call('hget', key, name)
			end
			-- store value on temporary hash table
			--redis.call('hset', skey, id, value)
			tkeys[i] = skey .. id
			-- store value on temporary key
			redis.call('set', tkeys[i], value)
		end
		--bykey = skey .. '->*'
		bykey = skey .. '*'
		--redis.call('expire', skey, 5)
	elseif field == '' then
	    bykey = nil
	else
		bykey = bk .. ':obj:*->' .. field
	end
	if bykey then
	   sortargs = {'BY',bykey}
	end
	if start > 0 or stop > 0 then
		table.insert(sortargs, 'LIMIT')
		table.insert(sortargs, start)
		table.insert(sortargs, stop)
	end
	if alpha == 'ALPHA' then
		table.insert(sortargs, alpha)
	end
	if desc == 'DESC' then
		table.insert(sortargs, desc)
	end
	ids = redis.call('sort', rkey, unpack(sortargs))
	redis_delete(tkeys)
else
	if ordering == 'DESC' then
		ids = redis.call('zrevrange', rkey, start, stop)
	elseif ordering == 'ASC' then
		ids = redis.call('zrange', rkey, start, stop)
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
elseif table.getn(fields) == 1 and fields[1] == 'id' then
	result = ids
else
	result = {}
	for i,id in pairs(ids) do
		result[i] = {id, redis.call('hmget', bk .. ':obj:' .. id, unpack(fields))}
	end
end

-- handle related item loading
local related_items = {}
local related_fields = {}
for r,rel in ipairs(related) do
	local field_items = {}
	local field = rel['field']
	local fields = rel['fields']
	related_items[r] = {rel['name'], field_items, fields}
	-- A structure
	if rel['type'] == 'structure' then
		for i,res in ipairs(result) do
			local id = res[1]
			local fid = bk .. ':obj:' .. id .. ':' .. field
			field_items[i] = {id, redis_members(fid,true)}
		end
	else
		local processed = {}
		local j = 0
		local rbk = rel['bk']
		for i,res in ipairs(result) do
			local id = bk .. ':obj:' .. res[1]
			local rid = redis.call('hget', id, field)
			local val = processed[rid]
			if not val then
				j = j + 1
				processed[rid] = j
				if # fields > 0 then
					field_items[j] = {rid, redis.call('hmget', rbk .. ':obj:' .. rid, unpack(fields))}
				else
					field_items[j] = {rid, redis.call('hgetall', rbk .. ':obj:' .. rid)}
				end
			end
		end
	end
end

return {result, related_items}