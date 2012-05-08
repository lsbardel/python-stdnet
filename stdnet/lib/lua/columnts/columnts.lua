-- Not a number
local nan = 0/0
-- 9 bytes string for nil data
local nildata = string.char(0,0,0,0,0,0,0,0,0)

-- Column timeseries class
local columnts = {
    --
    -- Initialize
    init = function (self, key)
        self.key = key
        self.fieldskey = key .. ':fields'
    end,
    --
    -- field data key
    fieldkey = function (self, field)
        return self.key .. ':field:' .. field
    end,
    --
    -- all field names for this timeseries
    fields = function (self)
        return redis.call('smembers', self.fieldskey)
    end,
    --
    -- num fields
    num_fields = function (self)
        return redis.call('scard', self.fieldskey) + 0
    end,
    --
    -- a set of fields
    fields_set = function(self)
        local f = {}
        for _,name in ipairs(self:fields()) do
            f[name] = self:fieldkey(name)
        end
        return f
    end,
    --
    -- Length of timeseries
    length = function (self)
        return redis.call('zcard', self.key) + 0
    end,
    --
    -- Delete timeseries
    del = function(self)
        local keys = redis.call('keys', self.key .. '*')
        if # keys > 0 then
            redis.call('del',unpack(keys))
        end
    end,
    --
    -- Return the ordered list of times
    times = function (self)
        return redis.call('zrange', self.key, 0, -1)
    end,
    --
    -- The rank of timestamp in the timeseries
    rank = function (self, timestamp)
        return redis.call('zrank', self.key, timestamp)
    end,
    --
    -- Return the unpacked value of field at rank
    rank_value = function (self, rank, field)
        local r = 9*rank
        local value = redis.call('getrange',self:fieldkey(field),r,r+9)
        return self:unpack_value(value)
    end,
    --
    -- A representation of the timeseries as a dictionary.
    -- If only one field is available, values will be the field values
    -- otherwise it will be a dictionary of fields.
    -- *Values are unpacked*
    asdict = function(self, fields)
        if self:length() == 0 then
            return nil
        end
        local times, field_values = unpack(self:all(fields))
        local result = {}
        local field_name
        local count = 0
        for fname,field in pairs(field_values) do
            count = count + 1
            field_name = fname
        end
        if count == 1 then
            field_values = field_values[field_name]
            for i,time in ipairs(times) do
                result[time] = self:unpack_value(field_values[i])
            end
        else
            for i,time in ipairs(times) do
                fvalues = {}
                for fname,field in pairs(field_names) do
                    fvalues[fname] = self:unpack_value(field_values[fname][i])
                end
                result[time] = fvalues
            end
        end
        return result
    end,
    --
    -- Add a timeseries, multiplied by the given weight, to self
    addserie = function(self, ts, weight, fields, tsmul)
        local range = ts:range('zrange', 0, -1, fields)
        local times, field_values = unpack(range)
        return self:add(times, field_values, weight, tsmul)
    end,
    --
    -- shortcut for returning the whole range of a timeserie
    all = function(self, fields)
        return self:range('zrange', 0, -1, fields)
    end,
    --
    -- remove a field and return true or false
    popfield = function (field)
        return redis.call('del', self:fieldkey(field))['ok'] + 0 == 1
    end,
    --
    -- remove a timestamp from timeseries and return it
    poptime = function(self, timestamp)
        local rank = redis.call('zrank', self.key, timestamp)
        if rank then
            rank = rank + 0
            for i,field in pairs(fields) do
                fieldid = tsid .. ':field:' .. field
                data = redis.call('getrange', fieldid, (rank+1)*9, -1)
                if rank > 0 then
                    data = redis.call('getrange', fieldid, (rank-1)*9, rank*9) + data
                end
                redis.call('set', fieldid, data)
            end
        end
    end,
    --
    -- return an array containg a range of the timeseries. The array
    -- contains two elements, an array of times and a dictionary of fields.
    range = function(self, command, start, stop, fields, unpack_values)
        local times = redis.call(command, self.key, start, stop)
        local field_values = {}
        local data = {times, field_values}
        local len = # times
        if len == 0 then
            return data
        end
        -- get the start rank (Also when we use tsrange. Important)
        start = redis.call('zrank', self.key, times[1])
        stop = start + len
        if not fields or # fields == 0 then
            fields = self:fields()
        end
        -- loop over fields
        for i,field in ipairs(fields) do
            local fkey = self:fieldkey(field)
            if redis.call('exists', fkey) + 0 == 1 then
                -- Get the string between start and stop from redis
                local sdata = redis.call('getrange', fkey, 9*start, 9*stop)
                local fdata = {}
                local p = 0
                if unpack_values then
                    local v
                    while p < 9*len do
                        v = self:unpack_value(string.sub(sdata, p+1, p+9))
                        table.insert(fdata,v)
                        p = p + 9
                    end
                else
                    while p < 9*len do
                        table.insert(fdata,string.sub(sdata, p+1, p+9))
                        p = p + 9
                    end
                end
                field_values[field] = fdata
            end
        end
        return data
    end,
    --
    -- Add/replace field values. If weights are provided, the values in
    -- field_values are already unpacked and they are added to existing
    -- values, otherwise the values are to be replaced. tsmul is an optional
    -- single field timeseries which multiply each field in self.
    --
    -- times is a table containing times
    -- field_values is a dictionary of field-values where values have the same
    -- 		length as *times*
    add = function (self, times, field_values, weights, tsmul)
        local fields = self:fields_set()
        local tslen = self:length() + 0
        local ws = {}
        local fkey, data, rank, rank9, available, weight, value, dvalue, v1, mul
        local new_serie = tslen == 0
        local time_set = {}
        if tsmul then
            assert(tsmul:length() > 0, 'Timeseries ' .. tsmul.key .. ' not available')
            assert(tsmul:num_fields() == 1, 'Timeseries ' .. tsmul.key .. ' has more than one field. Cannot be used tu multiply timeseries.')
            tsmul = tsmul:asdict()
        end
        
        -- Make sure all fields are available and have same data length
        for field, value in pairs(field_values) do
            -- add field to the fields set
            if not fields[field] then
            	fkey = self:fieldkey(field)
                redis.call('sadd', self.fieldskey, field)
                fields[field] = fkey
                if tslen > 0 then
                    redis.call('set', fkey, string.rep(nildata, tslen))
                end
            end
        end
        
        -- If we are adding to an existing timeseries
        -- Fill the time_set dictionary with false values at self:times
        if weights and not new_serie then
            local times = self:times()
            for index, timestamp in ipairs(times) do
                time_set[timestamp] = false
            end
        end
        
        -- Loop over times
        for index, timestamp in ipairs(times) do
            time_set[timestamp] = true
            available = self:rank(timestamp)
            -- This is a new timestamp
            if not available then
                redis.call('zadd', self.key, timestamp, timestamp)
                rank = redis.call('zrank', self.key, timestamp) + 0
                rank9 = 9*rank
                tslen = self:length()
                -- loop over all fields and append/insert new data to the field strings
                for field, fkey in pairs(fields) do
                    -- not at the end of the string! inefficient insertion.
                    if rank < tslen - 1 then
                        data = nildata .. redis.call('getrange', fkey, rank9, -1)
                    else
                        data = nildata
                    end 
                    redis.call('setrange', fkey, rank9, data)
                end
            -- No need to insert a new timestamp
            else
                rank = available + 0
                rank9 = 9*rank
            end
            
            -- Loop over field/values pairs
	        for field, values in pairs(field_values) do
	            -- add field to the fields set
	            fkey = fields[field]
	            value = values[index]
	            -- set the field value
	            if weights then
	                if type(weights) == 'number' then
	                    weight = weights
	                else
	                    weight = weights[field]
	                end
	                dvalue = weight*self:unpack_value(value)
	                if tsmul then
	                   mul = tsmul[timestamp]
	                   if mul then
	                       dvalue = mul*dvalue
	                   else
	                       dvalue = nan
	                   end
	                end
	                -- If the value is a number
	                if dvalue == dvalue then
	                    -- If the field was available add to the current value
	                    if not new_serie then
	                        v1 = redis.call('getrange', fkey, rank9, rank9+9)
	                        dvalue = dvalue + self:unpack_value(v1)
	                    end
	                    value = self:pack_value(dvalue)
	                else
	                    value = self:pack_value(dvalue)
	                end
	            elseif string.len(value) > 9 then
	                key = string.sub(value, 2, 9)
	                redis.call('set', self.key .. ':key:' .. key, string.sub(value, 10))
	                value = string.sub(value, 1, 9)
	            end
	            redis.call('setrange', fkey, rank9, value)
	        end
	    end
	    
	    if weight then
	        for timestamp, avail in pairs(time_set) do
	            if not avail then
	               rank9 = redis.call('zrank', self.key, timestamp)*9
	               for field, fkey in pairs(fields) do 
                        redis.call('setrange', fkey, rank9, nildata)
	               end
	           end
	       end
	    end
    end,

    --
    ----------------------------------------------------------------------------
    -- INTERNAL FUNCTIONS
    ----------------------------------------------------------------------------
    --
    -- unpack a single value
    unpack_value = function (self, value)
        local byte = string.byte(value)
        --assert(string.len(value) == 9, 'Invalid string to unpack. Length is ' .. string.len(value))
        if byte == 0 then
            return nan
        elseif byte == 1 then
            return struct.unpack('>i',string.sub(value,2,5))
        elseif byte == 2 then
            return struct.unpack('>d',string.sub(value,2))
        elseif byte == 3 then
            return string.sub(value,3,3+string.byte(value,2))
        else
            assert(byte == 4, 'Invalid string to unpack. First byte is ' .. byte)
            return self:string_value(string.sub(value,2))
        end
    end,
    --
    -- pack a single value
    pack_value = function (self, value)
        if type(value) == 'number' then
            if value == value then
                return string.char(2) .. struct.pack('>d', value)
            else
                return nildata
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

local columnts_meta = {}
-- Constructor
function columnts:new(key)
    local result = {}
    if type(key) == 'table' then
        for i,k in ipairs(key) do
            result[i] = columnts:new(k)
        end
        return result
    elseif type(key) == 'number' then
        return key + 0
    else
        for k,v in pairs(columnts) do
            result[k] = v
        end
        result:init(key)
        return setmetatable(result, columnts_meta)
    end
end


--
-- merge timeseries
-- elements: an array of dictionaries containing the weight and an array of timeseries to multiply
-- fields: list of fiedls to merge
-- Multiply timeseries. At the moment this only works for two timeseries
-- ts1*ts2, with ts1 being a one field timeseries and ts2 being and N-fields timeseries.
-- It multiplies the field in ts1 for each fields in ts2 and store the result at key
-- with fields names given by ts2.
function columnts:merge(key, elements, fields)
    local result = columnts:new(key)
    result:del()
    -- First we copy the first timeseries across to self
    for i,elem in ipairs(elements) do
        assert( # elem.series <= 2, 'Too many timeseries. Cannot perform operation')
        -- More than one timeseries. Create the timeseries obtain by multiplying them
        local ts,tsmul = elem.series[1]
        if # elem.series == 2 then
            tsmul = elem.series[1]
            ts = elem.series[2]
        end
        result:addserie(ts, elem.weight, fields, tsmul)
    end
    return result
end
