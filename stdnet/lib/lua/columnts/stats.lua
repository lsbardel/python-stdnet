-- Univariate and multivariate statistics on redis
local stats = {}

if not (KEYS and ARGV) then
    tabletools = require('tabletools')
end

local function add_field_names(key, field_values, serie_names)
    local fields = {}
    for field, values in pairs(field_values) do
        table.insert(fields, field)
        table.insert(serie_names, key .. ' @ ' .. field)
    end
    return fields
end

local function add_cross_section(section, index, field_values, fields)
    local v, field
    for _, field in ipairs(fields) do
        v = field_values[field][index]
        if v == v then
            table.insert(section, v)
        else
            return nil
        end
    end
    return section
end

-- vector1 += vector2
stats.vector_sadd = function (vector1, vector2)
    for i, v in ipairs(vector1) do
        vector1[i] = v + vector2[i]
    end
    return vector1
end

-- vector1 - vector2
stats.vector_diff = function (vector1, vector2)
    local result = {}
    for i, v in ipairs(vector1) do
        result[i] = v - vector2[i]
    end
    return result
end

-- Squared of a vector
stats.vector_square = function (vector)
    local vector2 = {}
    local n = 0
    for i, v in ipairs(vector) do
        for j = 1, i do
            n = n + 1
            vector2[n] = v*vector[j]
        end
    end
    return vector2
end

--
-- Calculate aggregate statistcs for a timeseries slice
stats.univariate = function (serie)
    local times = serie.times
    local sts = {}
    local N = # times
    if N == 0 then
        return sts
    end
    local result = {start=times[1], stop=times[N], len=N, stats=sts} 
    for field, values in pairs(serie.field_values) do
    	local dv, dv2
        local N = 0
        local min_val = 1.e10
        local max_val =-1.e10
        local sum_val = 0
        local sum2_val = 0
        local dsum, dsum2, dsum3, dsum4 = 0, 0, 0, 0
        local p = nan
        for i,v in ipairs(values) do
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
            sts[field] = {N=N,
                          min=min_val,
                          max=max_val,
                          sum=sum_val/N,
                          sum2=sum2_val/N,
                          dsum=dsum/(N-1),
                          dsum2=dsum2/(N-1),
                          dsum3=dsum3/(N-1),
                          dsum4=dsum4/(N-1)}
        end
    end
    return result
end

stats.fields_and_times = function (series)
    local times, serie, time, i, j, section
    local serie_names = {}
    local time_dict = {}
    -- Fill fields
    for i, serie in ipairs(series) do
        local fields = add_field_names(serie.key, serie.field_values, serie_names)
        if i == 1 then
            times = serie.times
            for j, time in ipairs(times) do
                time_dict[time .. ''] = add_cross_section({}, j, serie.field_values, fields)
            end
        else
            for j, time in ipairs(serie.times) do
                time = time .. ''
                local section = time_dict[time] 
                if section then
                    time_dict[time] = add_cross_section(section, j, serie.field_values, fields)
                end
            end
        end
    end
    return {times=times, names=serie_names, time_dict=time_dict}
end

--
-- Calculate aggregate statistcs for a timeseries slice
stats.multivariate = function (series)
    local prev_section, section, section2, dsection, start, stop
    local a = stats.fields_and_times(series)
    local time_dict = a.time_dict
    local S = # a.names
    local T = S*(S+1)/2
    local N = 0
    local sum   = tabletools.init(S, 0)
    local sum2  = tabletools.init(T, 0)
    local dsum  = tabletools.init(S, 0)
    local dsum2 = tabletools.init(T, 0)
    for i, time in ipairs(a.times) do
        section = time_dict[time .. '']
        if section and # section == S then
            N = N + 1
            stop = time
            stats.vector_sadd(sum, section)
            stats.vector_sadd(sum2, stats.vector_square(section))
            if prev_section then
                dsection = stats.vector_diff(section, prev_section)
                stats.vector_sadd(dsum, dsection)
                stats.vector_sadd(dsum2, stats.vector_square(dsection))
            else
                start = time
            end
            prev_section = section
        end
    end
    if N > 1 then
        return {fields=a.names, start=start, stop=stop, type='multi',
                N=N, sum=sum, sum2=sum2, dsum=dsum, dsum2=dsum2}
    end
end

stats.get_series = function ()
    local command = ARGV[1]
    local start = ARGV[2]
    local stop = ARGV[3]
    local calc_type = ARGV[4]
    local pos = 5
    local series = {}
    -- loop over keys
    for i, id in ipairs(KEYS) do
        local serie = columnts:new(id)
        local num_fields = ARGV[pos]
        local fields = tabletools.slice(ARGV, pos+1, pos+num_fields)
        pos = pos + num_fields + 1
        local time_values = serie:range(command, start, stop, fields, true)
        local t,v = unpack(time_values)
        table.insert(series, {key=id, times=t, field_values=v})
    end
    if # series == 0 then
        error('No timeseries available')
    end
    return {calc_type=calc_type, series=series}
end

if KEYS and ARGV then
    local r = stats.get_series()
    local result
    if r.calc_type == 'multi' then
        result = stats.multivariate(r.series)
    else
        result = stats.univariate(r.series[1])
    end
    if result then
        result = cjson.encode(result)
    end
    return result
else
    return stats
end