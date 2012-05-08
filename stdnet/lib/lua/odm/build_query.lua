-- Script to aggregate a stdnet query
local bk = KEYS[1] -- base key for model
local rkey = KEYS[2] -- the key where to store the structure containing the resuls
local s = ARGV[1] -- 's' for set or 'z' for sorted sets
local name = ARGV[2] -- Field name
local unique = ARGV[3] -- 'u' if field is unique '' otherwise
local lookup = ARGV[4] -- Not yet used


-- Perform the union of the index for value val and the result key *rkey*
-- The result is stored in *rkey*
local function union (val)
	local setkey = bk .. ':idx:' .. name .. ':' .. val
	if s == 's' then
	    redis.call('sunionstore', rkey, rkey, setkey)
	else
	    redis.call('zunionstore', rkey, 2, rkey, setkey)
	end
end

-- add a value to a 'rkey' set if the value is in the 'idset'
local function add (val)
	if val ~= false then
	    local idset = bk .. ':id'
		if s == 's' then
		    if redis.call('sismember',idset,val) + 0 == 1 then 
			    redis.call('sadd', rkey, val)
			end
		else
			local score = redis.call('zscore', idset, val)
			if score ~= false then
				redis.call('zadd', rkey, score, val)
			end
		end
	end
end

-- Add values stored at key to the 'rkey' set. `oper` is the operation
-- to perform for the values in container at `key` (either add or union)
local function addkey(key, oper)
    local processed = {}
    for _,v in ipairs(redis_members(key)) do
        if not processed[v] then
            oper(v)
        end
        processed[v] = true
    end
end

local i = 4
local what
local val
while i < # ARGV do
	what = ARGV[i+1] -- what type of value is val, either a key or an actual value
	val = ARGV[i+2]
	i = i + 2
	if unique == 'u' and name == 'id' then
		if what == 'key' then
		    addkey(val,add)
		else
			add(val) -- straightforward add of a member
		end
	elseif unique == 'u' then
		-- Unique field but not an id. These fields maps to ids in an hash table
		local mapkey = bk .. ':uni:' .. name
		if what == 'key' then
			-- This lookup is quite rare
			if s == 's' then
				for _,v in ipairs(redis.call('smembers', val)) do
					add(redis.call('hget', mapkey, v))
				end
			else
				for _,v in ipairs(redis.call('zrange', val, 0, -1)) do
					add(redis.call('hget', mapkey, v))
				end
			end
		else
			add(redis.call('hget', mapkey, val))
		end
	elseif what == 'key' then
	    addkey(val, union)
	else
		union(val)
	end
end

return redis.call(s .. 'card', rkey)