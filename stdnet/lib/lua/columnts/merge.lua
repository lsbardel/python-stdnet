-- perform a linear combination of one or more timeseries and store the
-- result in a new timeseries
-- result = weight1*ts11*ts12*.. + weight2*ts21*ts22*.. + ..
local tsdest = KEYS[1]    --  Destination timeseries
local j = 0
local idx = 1
local num_series = ARGV[idx] + 0 -- Number of linear combination block
local elements = {}
-- In each block there is a numeric weight and a group of timeseries
-- to multiply
while j < num_series do
    local tss = {}
    j = j + 1
    elements[j] = {weight = ARGV[idx+1] + 0, series = tss}
    local nseries = ARGV[idx+2] + 0
    idx = idx + 2
    local stop = idx + nseries
    while idx < stop do
        table.insert(tss, columnts:new(ARGV[idx+1]))
        idx = idx + 1
    end
    if # tss == 0 then
        return {err = 'No timeseries given to merge ' .. nseries}
    end
end
local fields = tabletools.slice(ARGV, idx+1, -1)

local ts = columnts:merge(tsdest, elements, fields)
return ts:length()
