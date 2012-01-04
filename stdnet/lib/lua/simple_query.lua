-- Perform a simple query. A simple query is a query on ids and unique fields
-- For example:
--	session.query(MyModel).get(id = 5)
--	session.query(MyModel).filter(id__in = (5,6,9))
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

local bk = KEYS[1] -- base key for model
local s = KEYS[2] -- 's' for set or 'z' for sorted sets
local rkey = KEYS[3] -- the key where to store the result ids
local add = s .. 'add'
local idkey = bk .. ':id'
local N = table.getn(KEYS)
local i = 3
local ids = {}
local j = 0
while i < N  do
    local field = KEYS[i+1]
    local i0 = i+2
    local len = KEYS[i0]
    local values = table_slice(KEYS, i0+1, i0+len)
    i = i0 + len
    if field ~= 'id' then
    	ukey = bk .. ':uni:' .. field
    	for i,v in pairs(values) do
    		id = redis.call('hget',key,v)
    		if id ~= false then
    			j = j + 1
    			ids[j] = id
    		end
    	end
    else
    	for i,id in pairs(values) do
    		j = j + 1
    		ids[j] = id
    	end
    end
end

if s == 's' then
	for _,id in pairs(ids) do
		if redis.call('ismember', idkey, id) then
			redis.call(add, rkey, id)
		end
	end
else
	for _,id in pairs(ids) do
		local score = redis.call('zscore', idkey, id)
		if score ~= false then
			redis.call(add, rkey, score, id)
		end
	end
end

return redis.call(s .. 'card', rkey)