-- Script to aggregate a stdnet query
bk = KEYS[1] -- base key for model
rkey = KEYS[2] -- the key where to store the structure containing the resuls
s = ARGV[1] -- 's' for set or 'z' for sorted sets
name = ARGV[2]
unique = ARGV[3]
lookup = ARGV[4]

-- Perform the union of the index for value val and the result key *rkey*
-- The result is stored in *rkey*
function union (val)
	local setkey = bk .. ':idx:' .. name .. ':' .. val
	if s == 's' then
	    redis.call('sunionstore', rkey, rkey, setkey)
	else
	    redis.call('zunionstore', rkey, 2, rkey, setkey)
	end
end

-- add a value to a set (or sorted set)
function add (val)
	if val ~= false then
	    idset = bk .. ':id'
		if s == 's' then
		    if redis.call('sismember',idset,val) + 0 == 1 then 
			    redis.call('sadd', rkey, val)
			end
		else
			score = redis.call('zscore', idset, val)
			if score ~= false then
				redis.call('zadd', rkey, score, val)
			end
		end
	end
end

i = 4
local what
local val
while i < # ARGV do
	what = ARGV[i+1] -- what type of value is val, either a key or an actual value
	val = ARGV[i+2]
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