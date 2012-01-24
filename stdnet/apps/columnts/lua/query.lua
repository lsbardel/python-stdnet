local tsid = KEYS[1]	--	timeseries id
local command = KEYS[2] -- either tsrange or tsrangebytime
local start = KEYS[3] + 0
local stop = KEYS[4] + 0
local index = 5
local num_fields = KEYS[index] + 0-- number of fields to extract. If 0 all fields will be extracted

function tsfields(id)
	local fields = {}
	local idf =  id .. ':field:'
	local start = string.len(idf) + 1
	for _,k in pairs(redis.call('keys', idf .. '*')) do
		table.insert(fields,string.sub(k,start))
	end
	return fields
end

if num_fields == 0 then
	fields = tsfields(tsid)
else
	fields = table_slice(KEYS, index+1, index+num_fields)
	index = index + num_fields
end
local times = redis.call(command, tsid, start, stop, 'novalues')
local len = # times
if len == 0 then
	return times
elseif command == 'tsrangebytime' then
	start = redis.call('tsrank', tsid, start)
end
stop = start + len
local data = {times}

for i,field in pairs(fields) do
	local fid = tsid .. ':field:' .. field
	if redis.call('exists', fid) then
		local stop = 9
		local sdata = redis.call('getrange', fid, 9*start, 9*stop)
		local fdata = {}
		local p = 0
		while p < 9*len do
			table.insert(fdata,string.sub(sdata, p+1, p+9))
			p = p + 9
		end
		table.insert(data,field)
		table.insert(data,fdata)
	end
end

return data