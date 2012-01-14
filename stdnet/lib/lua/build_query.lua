-- Script to aggregate a stdnet query
bk = KEYS[1] -- base key for model
s = KEYS[2] -- 's' for set or 'z' for sorted sets
rkey = KEYS[3] -- the key where to store the structure containing the resuls
unionstore = s .. 'unionstore'
name = KEYS[4]
unique = KEYS[5]
lookup = KEYS[6]

function union (val)
	local setkey = bk .. ':idx:' .. name .. ':' .. val
	redis.call(unionstore, rkey, rkey, setkey)
end

function add (val)
	if val ~= false then
		if s == 's' then
			redis.call('sadd', rkey, val)
		else
			score = redis.call('zscore', bk .. ':id', val)
			if score ~= false then
				redis.call('zadd', rkey, score, val)
			end
		end
	end
end

i = 6
local what
local val
while i < table.getn(KEYS) do
	what = KEYS[i+1] -- what type of value is val
	val = KEYS[i+2]
	i = i + 2
	if unique == 'u' and name == 'id' then
		if what == 'key' then
			union(val) -- straightforward union
		else
			add(val) -- straightforward add of a member
		end
	elseif unique == 'u' then
		-- Unique field but not an id. These fields maps to ids in an hash table
		mapkey = bk .. ':uni:' .. name
		if what == 'key' then
			-- This lookup is quite rare
			if s == 's' then
				for i,v in pairs(redis.call('smembers', val)) do
					add(redis.call('hget', mapkey, v))
				end
			else
				for i,v in pairs(redis.call('zrange', val, 0, -1)) do
					add(redis.call('hget', mapkey, v))
				end
			end
		else
			add(redis.call('hget', mapkey, val))
		end
	elseif what == 'key' then
	    -- An index with a key, The key may be a set, a zset or a list
		if s == 's' then
			for i,v in pairs(redis.call('smembers', val)) do
				union(v)
			end
		else
			for i,v in pairs(redis.call('zrange', val, 0, -1)) do
				union(v)
			end
		end
	else
		union(val)
	end
end

return redis.call(s .. 'card', rkey)