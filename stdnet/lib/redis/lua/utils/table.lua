-- Slice a lua table between i1 and i2
function table_slice (values,i1,i2)
    local res = {}
    local n = #values
    -- default values for range
    i1 = i1 or 1
    i2 = i2 or n
    if i2 < 0 then
        i2 = n + i2 + 1
    elseif i2 > n then
        i2 = n
    end
    if i1 < 1 or i1 > n then
        return {}
    end
    local k = 1
    for i = i1,i2 do
        res[k] = values[i]
        k = k + 1
    end
    return res
end

-- Convert a dictionary into a flat array. For example {bla = 'foo', planet = 'mars'}
-- becomes {'bla', 'foo', 'planet', 'mars'}
function flat_table(tbl)
    result = {}
    for name,value in pairs(tbl) do
        table.insert(result,name)
        table.insert(result,value)
    end
    return result
end

-- Return True if v is a number
function isnumber(v)
    return pcall(function() return v + 0 end)
end

function asnumber(v)
    if isnumber(v) then
        return v + 0
    else
        return nil
    end
end