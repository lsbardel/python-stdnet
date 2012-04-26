-- Initailize a vector with a value
function init_vector(size, value)
    local vector = {}
    for i = 1, size do
        vector[i] = value
    end
    return vector
end

function equal_vectors(v1, v2)
    if # v1 == # v2 then
        for i, v in ipairs(v1) do
            if v ~= v2[i] then
                return false
            end
        end
        return true
    else
        return false
    end
end

-- vector1 += vector2
function vector_sadd(vector1, vector2)
    for i, v in ipairs(vector1) do
        vector1[i] = v + vector2[i]
    end
    return vector1
end

-- vector1 - vector2
function vector_diff(vector1, vector2)
    local result = {}
    for i, v in ipairs(vector1) do
        result[i] = v - vector2[i]
    end
    return result
end

-- Squared of a vector
function vector_square(vector)
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
function uni_stats(serie)
    local times = serie.times
    local sts = {}
    local N = # times
    if N == 0 then
        return sts
    end
    local result = {start = times[1], stop = times[N], len = N, stats = sts} 
    for field, values in pairs(serie.field_values) do
        local N = 0
        local min_val = 1.e10
        local max_val =-1.e10
        local sum_val = 0
        local sum2_val = 0
        local dsum, dsum2, dsum3, dsum4 = 0, 0, 0, 0
        local p, dv = nan
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


function add_field_names(key, field_values, serie_names)
    local fields = {}
    for field, values in pairs(field_values) do
        name = key .. ' @ ' .. field
        table.insert(fields, field)
        table.insert(serie_names, name)
    end
    return fields
end

function add_cross_section(section, index, field_values, fields)
    local v
    for index, field in ipairs(fields) do
        v = field_values[field][index]
        if v == v then
            table.insert(section, v)
        else
            return nil
        end
    end
    return section
end

function fields_and_times(series)
    local times
    local section
    local serie_names = {}
    local time_dict = {}
    -- Fill fields
    for i, serie in ipairs(series) do
        local fields = add_field_names(serie.key, serie.field_values, serie_names)
        if i == 1 then
            times = serie.times
            for i, time in ipairs(times) do
                time_dict[time] = add_cross_section({}, i, serie.field_values, fields)
            end
        else
            for i, time in ipairs(serie.times) do
                local section = time_dict[time] 
                if section then
                    time_dict[time] = add_cross_section(section, i, serie.field_values, fields)
                end
            end
        end
    end
    return {times=times, names=serie_names, time_dict=time_dict}
end

--
-- Calculate aggregate statistcs for a timeseries slice
function multi_stats(series)
    local prev_section, section, section2, dsection
    local a = fields_and_times(series)
    local time_dict = a.time_dict
    local S = # a.names
    local T = S*(S+1)/2
    local N = 0
    local sum   = init_vector(S, 0)
    local sum2  = init_vector(T, 0)
    local dsum  = init_vector(S, 0)
    local dsum2 = init_vector(T, 0)
    for i, time in ipairs(a.times) do
        section = time_dict[time]
        if section then
            N = N + 1
            vector_sadd(sum, section)
            vector_sadd(sum2, vector_square(section))
            if prev_section then
                dsection = vector_diff(section, prev_section)
                prev_section = section
                vector_sadd(dsum, dsection)
                vector_sadd(dsum2, vector_square(dsection))
            end
        end
    end
    if N > 1 then
        return {fields = serie_names,
                npoints = N,
                sum = sum,
                sum2 = sum2,
                dsum = dsum,
                dsum2 = dsum2}
    end
end

function get_series()
    local command = ARGV[1]
    local start = ARGV[2]
    local stop = ARGV[3]
    local pos = 4
    local series = {}
    for i, id in ipairs(KEYS) do
        local num_fields = ARGV[pos]
        local serie = columnts:new(id)
        local num_fields = ARGV[pos]
        local fields = table_slice(ARGV, pos+1, pos+num_fields)
        pos = pos + num_fields + 1
        local time_values = serie:range(command, start, stop, fields, true)
        local t,v = unpack(time_values)
        table.insert({key=id, times=t, field_values=v})
    end
    if # series == 0 then
        error('No timeseries available')
    end
    return series
end

if KEYS then
    local series = get_series()
    if # series > 1 then
        stats = multi_stats(series)
        if stats then
            stats = cjson.encode(stats)
        end
    else
        stats = flat_table(uni_stats(series[1]))
        stats[4] = flat_table(stats[4])
    end
        
    return stats
end