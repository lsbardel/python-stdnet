-- perform a linear combination of one or more timeseries and store the
-- result in a new timeseries
local num_series = ARGV[1] + 0
local tsdest = KEYS[1]    --  Destination timeseries
local tskeys = table_slice(KEYS,2,2+num_series) -- Series to merge
local weights = table_slice(ARGV,2,2+num_series) -- Multiplication Weights
local fields = table_slice(ARGV,3+num_series,-1)

if # tskeys == 0 then
    return {err = 'No timeseries given'}
end

local ts = columnts.new(tsdest)
local tss = columnts.new(tskeys)
local weights = columnts.new(weights)
ts.merge(tss, weights, fields)

