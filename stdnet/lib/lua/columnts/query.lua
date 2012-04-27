local command = ARGV[1] -- either tsrange or tsrangebytime
local start = ARGV[2] + 0
local stop = ARGV[3] + 0
local novalues = ARGV[4] + 0
local delete = ARGV[5] + 0
local fields = tabletools.slice(ARGV, 7, 6+ARGV[6])
local ts = columnts:new(KEYS[1])
if novalues == 1 then
    return ts:times(command, start, stop)
else
    local time_values = ts:range(command, start, stop, fields)
    local result = {time_values[1]}
    for k,v in pairs(time_values[2]) do
        table.insert(result,k)
        table.insert(result,v)
    end
    if delete == 1 then
        ts:del()
    end
    return result
end