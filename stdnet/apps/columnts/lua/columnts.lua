local nan = 0/0
-- 9 bytes string for nil data
local nildata = string.char(0,0,0,0,0,0,0,0,0)

-- Column timeseries class
columnts = {
    --
    -- Initialize
    init = function (self,key)
        self.key = key
    end,
    --
    -- field key
    fieldkey = function (self, field)
        return self.key .. ':field:' .. field
    end,
    --
    -- all field names for this timeseries
    fields = function (self)
        return redis.call('smembers', self.key .. ':fields')
    end,
    --
    -- Length of timeseries
    length = function (self)
        return redis.call('tslen', self.key) + 0
    end,
    --
    -- merge timeseries
    -- tss: list of timeseries to merge
    -- fields: list of fiedls to merge
    -- weights: weights for each timeseries
    -- mask: flag indicating how to perform the merge. One of (skip, null, interpolate)
    merge = function (self, tss, fields, weights, mask)
        assert(# tss == # weights)
        -- First we copy the first timeseries across to self
        for i,ts in ipairs(tss) do
            local weight = weights[i]
            local time_fields = ts.all(fields)
            local time = time_fields[1]
            local field_values = time_fields[2]
            --for j,t in ipairs(time) do
            --    local fvals = self:unpack_values(i,field_values)
            --    self:add(t,fvals,weight)
            --end
        end
    end,
    --
    -- Return the ordered list of times
    times = function (self)
        return redis.call('tsrange', 0, -1, 'novalues')
    end,
    --
    -- The rank of timestamp in the timeseries
    rank = function (self, timestamp)
        return redis.call('tsrank', self.key, timestamp)
    end,
    --
    -- Return the unpacked value of field at rank
    rank_value = function (self, rank, field)
        local value = redis.call('getrange',self:fieldkey(field),9*rank,9*rank+9)
        return self:unpack_value(value)
    end,
    --
    -- Set field values at timestamp
    set = function (self, timestamp, field_values)
        return self:add_replace(timestamp, field_values, nil)
    end,
    --
    -- Add field values at time
    add = function (self, time, field_values, weights)
        return self:add_replace(timestamp, field_values, weights)
    end,
    --
    -- shortcut for returning the whole range of a timeserie
    all = function(self, fields)
        return self:range('tsrange', 0, -1, fields)
    end,
    --
    -- remove a field and return true or false
    popfield = function (field)
        return redis.call('del', self:fieldkey(field))['ok'] + 0 == 1
    end,
    --
    -- remove a timestamp from timeseries and return it
    poptime = function(self, timestamp)
        local rank = redis.call('tsrank', self.key, timestamp)
        if rank then
            rank = rank + 0
            for i,field in pairs(fields) do
                fieldid = tsid .. ':field:' .. field
                data = redis.call('getrange', fieldid, (rank+1)*9, -1)
                if rank > 0 then
                    data = redis.call('getrange', fieldid, (rank-1)*9, rank*9) + data
                end
                redis.call('set',fieldid,data)
            end
        end
    end,
    --
    -- return a table containg a range of the timeseries
    range = function(self, command, start, stop, fields)
        local times = redis.call(command, self.key, start, stop, 'novalues')
        local len = # times
        if len == 0 then
            return times
        elseif command == 'tsrangebytime' then
            start = redis.call('tsrank', self.key, start)
        end
        stop = start + len
        local field_values = {}
        local data = {times, field_values}
        if # fields == 0 then
            fields = self:fields()
        end

        for i,field in ipairs(fields) do
            local fkey = self:fieldkey(field)
            if redis.call('exists', fkey) then
                local stop = 9
                local sdata = redis.call('getrange', fkey, 9*start, 9*stop)
                local fdata = {}
                local p = 0
                while p < 9*len do
                    table.insert(fdata,string.sub(sdata, p+1, p+9))
                    p = p + 9
                end
                field_values[field] = fdata
            end
        end
        return data
    end,
    --
    -- unpack a single value
    unpack_value = function (self, value)
        local byte = string.byte(value)
        assert(byte <= 4, 'Invald string to unpack')
        if byte == 0 then
            return nan
        elseif byte == 1 then
            return struct.unpack('>i',string.sub(value,2,5))
        elseif byte == 2 then
            return struct.unpack('>d',string.sub(value,2))
        elseif byte == 3 then
            return string.sub(value,3,3+string.byte(value,2))
        else
            return self:string_value(string.sub(value,2))
        end
    end,
    --
    ----------------------------------------------------------------------------
    -- INTERNAL FUNCTIONS
    ----------------------------------------------------------------------------
    --
    -- Add/replace field values. If weights are provided, values are added, otherwise
    -- the are replaced.
    add_replace = function (self, timestamp, field_values, weights)
        local fields = self:fields()
        local fkey,data,available_rank,weight
        for field,value in pairs(field_values) do
            redis.call('sadd', self.key .. ':fields', field)
            if replace and string.len(value) > 9 then
                key = string.sub(2,9)
                value = string.sub(10)
                redis.call('set', self.key .. ':key:' .. key, value)
                value = string.sub(1,9)
            end
            available_rank = self:rank(timestamp)
            -- not there, we need to insert/append a new value (hopefully append!)
            if not available_rank then
                redis.call('tsadd', self.key, timestamp, 1)
                rank = redis.call('tsrank', self.key, timestamp)
                for i,fname in pairs(fields) do
                    fkey = self:fieldkey(fname)
                    data = nildata
                    -- not at the end of the string! inefficient insertion.
                    if rank < self:length() - 1 then
                        data = nildata .. redis.call('getrange', fkey, (rank+1)*9, -1)
                    end 
                    redis.call('setrange', fkey, 9*rank, data)
                end
            end
            -- set the field value
            if weight then
                weight = weights[field]
                value = weight*value
                if available_rank then
                    value = value + self:rank_value(available_rank,field)
                end
                value = self:pack_value(value)
            end
            redis.call('setrange', fieldid, 9*rank, value)
        end
    end,
    --
    -- pack a single value
    pack_value = function (self, value)
        if type(value) == 'number' then
            if value == value then
                return string.char(2) .. struct.pack('>d', value)
            else
                return nan
            end
        else
            local n = string.len(value)
            if n < 8 then
                return string.char(3) .. n .. value + string.sub(nildata,1,7-n)
            else
                return value
            end
        end
    end,
    --
    -- unpack values at a given index, it returns a dictionary
    -- of field - unpacked values pairs
    unpack_values = function (self, index, field_values)
        fields = {}
        for field,values in pairs(field_values) do
            local idx = 9*index
            local value = string.sub(values,idx+1,idx+9)
            fields[field] = self:unkpack_value(value)
        end
        return fields
    end,
    --
    -- string value for key
    string_value = function (self, key)
        return {key,redis.call('get', self.key .. ':key:' .. key)}
    end
}

columnts_meta = {}
-- Constructor
function columnts:new(key)
    local result = {}
    if type(key) == 'table' then
        for i,k in ipairs(key) do
            result[i] = columns:new(k)
        end
        return result
    else
        for k,v in pairs(columnts) do
            result[k] = v
        end
        result:init(key)
        return setmetatable(result, columnts_meta)
    end
end

