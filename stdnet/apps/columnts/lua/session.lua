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
local tslen = redis.call('tslen', tsid) + 0

-- 9 bytes string for nil data
nildata = string.char(0,0,0,0,0,0,0,0,0)

function tsfields()
	return redis.call('keys', tsid .. ':field:*')
end
 
-- First we process timestamps to remove.
-- Removing timestamps is inefficient!!! Use with care.
local fields = tsfields()
local index = 1
local num_remove = ARGV[index] + index
while index < num_remove do
	index = index + 1
	timestamp = ARGV[index]
	if tslen > 0 then
		rank = redis.call('tsrank', tsid, timestamp)
		if rank ~= false then
			rank = rank + 0
			for i,field in pairs(fields) do
				fieldid = tsid .. ':field:' .. field
				data = redis.call('getrange', fieldid, (rank+1)*9, -1)
				if rank > 0 then
					data = redis.call('getrange', fieldid, (rank-1)*9, rank*9) + data
				end
				redis.call('set',fieldid,data)
			end
		end
	end
end

-- Second remove fields
index  = index + 1
local num_strings = ARGV[index] + index
while index < num_strings do
	field = ARGV[index+1]
	index = index + 1
	redis.call('del',  tsid .. ':field:' .. field)
end

-- Last we process data to add by looping over data fields
local fields = tsfields()
while index < # ARGV do
	field = ARGV[index+1]
	fieldid = tsid .. ':field:' .. field
	index = index + 2
	len_data = index + 2*ARGV[index]
	if len_data > 0 and not redis.call('exists',fieldid) then
		table.insert(fields,field)
	end
	while index < len_data do
		timestamp = ARGV[index+1] + 0
		value = ARGV[index+2]
		index = index + 2
		-- Storing a string if value is bigger than 9 bits
		if string.len(value) > 9 then
			key = string.sub(2,9)
			value = string.sub(10,-1)
			redis.call('set', tsid .. ':keys:' .. key, value)
			value = string.sub(1,9)
		end
		local rank = false
		if tslen > 0 then
			rank = redis.call('tsrank', tsid, timestamp)
		end
		-- not there, we need to insert/append a new value (hopefully append!)
		if not rank then
			redis.call('tsadd', tsid, timestamp, 1)
			rank = redis.call('tsrank', tsid, timestamp)
			tslen = redis.call('tslen', tsid) + 0
			for i,fname in pairs(fields) do
				fid = tsid .. ':field:' .. fname
				data = nildata
				-- not at the end of the string! inefficient insertion.
				if rank < tslen - 1 then
					data = nildata .. redis.call('getrange', fid, (rank+1)*9, -1)
				end	
				redis.call('setrange', fid, 9*rank, data)
			end
		end
		-- set the field value
		redis.call('setrange', fieldid, 9*rank, value)
	end
end
		
return redis.call('tslen', tsid)

