-- perform a linear combination of one or more timeseries and store the
-- result in a new timeseries
local num_fields = ARGV[1] + 0
local mask = ARGV[2] -- How to handle missing values, one of skip, null, interpolate.
local tsdest = KEYS[1]    --  Destination timeseries
local tskeys = table_slice(KEYS,2,-1) --  Destination timeseries
local fields = table_slice(ARGV,3,-1)

if # tskeys == 0 then
    return {err = 'No timeseries given'}
end

local ts = columnts.new(tsdest)
local tss = columnts.new(tskeys)
ts.merge(tss,fields,mask)

