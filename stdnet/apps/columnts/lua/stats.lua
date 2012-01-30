local start = ARGV[1]
local stop = ARGV[2]
local num_fields = ARGV[3]
local fields = table_slice(ARGV, 4, -1)
local ts = columnts:new(KEYS[1])
local stats = flat_table(ts:stats(start, stop, fields))
stats[4] = flat_table(stats[4])
return stats