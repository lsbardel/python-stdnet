local ts = columnts:new(KEYS[1])	--	timeseries

-- First remove timestamps
local index = 1
local num_remove = ARGV[index] + index
while index < num_remove do
	index = index + 1
	ts:poptime(ARGV[index])
end

-- Second remove fields
index  = index + 1
local num_strings = ARGV[index] + index
while index < num_strings do
	field = ARGV[index+1]
	index = index + 1
	ts:popfield(field)
end

-- Last we process data to add by looping over data fields
while index < # ARGV do
	local field = ARGV[index+1]
	local field_value = {}
	index = index + 2
	local len_data = index + 2*ARGV[index]
	while index < len_data do
		field_value[field] = ARGV[index+2]
		ts:set(ARGV[index+1] + 0, field_value)
		index = index + 2
	end
end
		
return ts:length()

