local command = ARGV[1]
local start = ARGV[2]
local stop = ARGV[3]
local num_fields = ARGV[4]
local fields = table_slice(ARGV, 5, -1)
local ts = columnts:new(KEYS[1])

--
-- Calculate aggregate statistcs for a timeseries slice
function stats(self, command, start, stop, fields)
    local time_values = self:range(command, start, stop, fields, true)
    local times, field_values = unpack(time_values)
    local sts = {}
    local N = # times
    if N == 0 then
        return sts
    end
    local result = {start = times[1], stop = times[N], len = N, stats = sts}
    -- Loop over field-value pairs 
    for field, values in pairs(field_values) do
        local N = 0
        local min_val = 1.e10
        local max_val =-1.e10
        local sum_val = 0
        local sum2_val = 0
        local dsum, dsum2, dsum3, dsum4 = 0, 0, 0, 0
        local p, dv = nan
        -- loop over values
        for _, v in ipairs(values) do
            if v == v then
                min_val = math.min(min_val, v)
                max_val = math.max(max_val, v)
                sum_val = sum_val + v
                sum2_val = sum2_val + v*v
                if p == p then
                    dv = v - p
                    dv2 = dv*dv
                    dsum = dsum + dv
                    dsum2 = dsum2 + dv2
                    dsum3 = dsum3 + dv2*dv
                    dsum4 = dsum4 + dv2*dv2
                end
                p = v
                N = N + 1
            end
        end
        if N > 1 then
            sts[field] = {'N',N,
                          'min',min_val .. '',
                          'max',max_val .. '',
                          'sum',sum_val/N .. '',
                          'sum2',sum2_val/N .. '',
                          'dsum',dsum/(N-1) .. '',
                          'dsum2',dsum2/(N-1) .. '',
                          'dsum3',dsum3/(N-1) .. '',
                          'dsum4',dsum4/(N-1) .. ''}
        end
    end
    return result
end

local stats = flat_table(stats(ts, command, start, stop, fields))
stats[4] = flat_table(stats[4])
return stats