local command = ARGV[1] -- either tsrange or tsrangebytime
local start = ARGV[2] + 0
local stop = ARGV[3] + 0
local fields = table_slice(ARGV, 5, 4+ARGV[4])
local ts = columnts:new(KEYS[1])
local time_values = ts:range(command, start, stop, fields)
local result = {time_values[1]}
for k,v in pairs(time_values[2]) do
    table.insert(result,k)
    table.insert(result,v)
end
return result