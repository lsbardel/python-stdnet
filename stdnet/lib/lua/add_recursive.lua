-- Script to aggregate recursive fields
local bk = KEYS[1] -- base key for model
local s = KEYS[2] -- 's' for set or 'z' for sorted sets
local rkey = KEYS[3] -- the key where to store the structure containing the resuls
local field = KEYS[4]
local idset = bk .. ':id'

-- add a value to a set (or sorted set)
function add (val)
    if s == 's' then
        redis.call('sadd', rkey, val)
    else
        score = redis.call('zscore', idset, val)
        if score ~= false then
            redis.call('zadd', rkey, score, val)
        end
    end
end

function remove(ids, toadd)
	for _,id in ipairs(ids) do
	    if toadd then
	       add(id)
	    end
	    remove(redis_members(bk .. ':idx:' .. field .. ':' .. id),true) 
	end
end


remove(redis_members(rkey),false)
