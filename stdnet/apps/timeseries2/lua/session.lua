-- Update a timeseries session
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

local tsid = KEYS[1]	--	timeseries id
local N = table.getn(KEYS)
local fields = redis.call('smembers', tsid .. ':fields')

nildata = '\x00\x00\x00\x00\x00\x00\x00\x00\x00'

-- First we process timestamps to remove
-- removing timestamps is inefficient!!! Use with care.
local index = 1
local num_remove = KEYS[index] + index
while index < num_remove do
	index += 1
	timestamp = KEYS[index]
	rank = redis.call('tsrank', tsid, timestamp)
	if rank ~= false then
		rank = rank + 0
		for i,field in  pairs(fields) do
			fieldid = tsid .. ':field:' .. field
			data = redis.call('getrange', fieldid, (rank+1)*9, -1)
			if rank > 0:
				data = redis.call('getrange', fieldid, (rank-1)*9, rank*9) + data
			end
			redis.call('set',fieldid,data)
		end
	end
end
			
-- Second remove fields
local num_strings = KEYS[index] + index
while index < num_strings do
	field = KEYS[index+1]
	index = index + 1
	redis.call('srem',field)
	redis.call('del',  tsid .. ':field:' .. field)
end

-- Last we process data to add
while index < N do
	field = KEYS[index+1]
	fieldid = tsid .. ':field:' .. field
	index = index + 2
	len_data = index + 2*KEYS[index]
	while index < len_data do
		timestamp = KEYS[index+1]
		value = KEYS[index+2]
		index = index + 2
		-- Storing a string if value is bigger than 9 bits
		if string.len(value) > 9 then
			key = string.sub(2,9)
			value = string.sub(10,-1)
			redis.call('set', tsid .. ':keys:' .. key, value)
			value = string.sub(1,9)
		rank = redis.call('tsrank', tsid, timestamp)
		-- not there, we need to insert/append a new value (hopefully append!)
		if rank == false do
			redis.call('tsadd', tsid, timestamp, 1)
			rank = redis.call('tsrank', tsid)
			len = redis.call('tslen', tsid) + 0
			for i,fname in  pairs(fields) do
				fid = tsid .. ':field:' .. fname
				data = nildata
				-- not at the end of the string! inefficient insertion.
				if rank < len - 1 then
					data = nildata ... redis.call('getrange', fid, (rank+1)*9, -1)
				end	
				redis.call('setrange', fid, 9*rank, data)
			end
		end
		redis.call('setrange', fieldid, 9*rank, value)
	end
end
		
return redis.call('tslen', tsid)

