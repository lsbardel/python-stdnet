local AUTO_ID, COMPOSITE_ID, CUSTOM_ID = 1, 2, 3
-- odm namespace - object-data mapping
local odm = {
    redis=nil,
    TMP_KEY_LENGTH = 12,
    ModelMeta = {
        namespace = '',
        id_type = AUTO_ID,
        id_name = 'id',
        id_fields = {},
        multi_fields = {},
        sorted = false,
        autoincr = false,
        indices = {}
    },
    range_selectors = {
        ge = function (v, v1)
            return v >= v1
        end,
        gt = function (v, v1)
            return v > v1
        end,
        le = function (v, v1)
            return v <= v2
        end,
        lt = function (v, v1)
            return v < v1
        end,
        startswith = function (v, v1)
            return string.sub(v, 1, string.len(v1)) == v1
        end,
        endswidth = function (v, v1)
            return string.sub(v, string.len(v) - string.len(v1) + 1) == v1
        end,
        contains = function (v, v1)
            return string.find(v, v1) ~= nil 
        end
    }
}
-- Model pseudo-class
odm.Model = {
    --[[
     Initialize model with model MetaData table
    --]]
    init = function (self, meta)
        self.meta = meta
        self.idset = self.meta.namespace .. ':id'    -- key for set containing all ids
        self.auto_ids = self.meta.namespace .. ':ids' -- key for auto ids
    end,
    --[[
     Commit an instance to redis
        num: number of instances to commit
        args: table containing data to save. The data is on the form
            action, id, score, data_length
        @return an array of id saved to the database
    --]]
    commit = function (self, num, args)
        local count, p, results = 0, 0, {}
        while count < num do
            local action, id, score, idx0 = args[p+1], args[p+2], args[p+3], p+4
            local length_data = args[idx0] + 0
            local data = tabletools.slice(args, idx0+1, idx0+length_data)
            count = count + 1
            p = idx0 + length_data
            results[count] = self:_commit_instance(action, id, score, data)
        end
        return results
    end,
    --[[
        Build a new query and store the resulting ids into a temporary set.
        It returns a dictionary containing the temporary set key and the
        size of the temporary set
    --]]
    query = function (self, field, tmp, queries)
        local ranges, field_type = {}, self.meta.indices[field]
        for _, q in ipairs(queries) do
            if q.type == 'set' then
                self:_queryset(tmp, field, field_type, q.value)
            elseif q.type == 'value' then
                self:_queryvalue(tmp, field, field_type, q.value)
            else
                -- Range queries are processed together
                selector = range_selectors[q.type]
                if selector then
                    table.insert(ranges, {selector=selector, v1=q.value})
                else
                    error('Cannot understand query type "' .. q.type .. '".')
                end
            end
        end
        if # ranges > 0 then
            self:_selectranges(tmp, field, field_type, ranges)
        end
        return {key=tmp, size=self:setsize(tmp)}
    end,
    --[[
        Delete a query stored in tmp id
    --]]
    delete = function (self, tmp)
        local ids, results = redis_members(tmp), {}
        for _, id in ipairs(ids) do
            if self:update_indices('delete', id) == 1 then
                table.insert(results, id)
            end
        end
        return results
    end,
    --
    --          INTERNAL METHODS
    --
    object_key = function (self, id)
        return self.meta.namespace .. ':obj:' .. id
    end,
    --
    mapkey = function (self, field)
        return self.meta.namespace .. ':uni:' .. field
    end,
    --
    index_key = function (self, field, value)
        local idxkey = self.meta.namespace .. ':idx:' .. field .. ':'
        if value then
            idxkey = idxkey .. value
        end
        return idxkey
    end,
    --
    temp_key = function (self)
        local bk = self.meta.namespace .. ':tmp:'
        while true do
            local chars = {}
            for loop = 1, odm.TMP_KEY_LENGTH do
                chars[loop] = string.char(math.random(1, 255))
            end
            local key = bk .. table.concat(chars)
            if odm.redis.call('exists', key) + 0 == 0 then
                return key
            end
        end
    end,
    --
    setsize = function(self, setid)
        if self.meta.sorted then
            return odm.redis.call('zcard', setid)
        else
            return odm.redis.call('scard', setid)
        end
    end,
    --
    setids = function(self, setid)
        if self.meta.sorted then
            return odm.redis.call('zrange', setid, 0, -1)
        else
            return odm.redis.call('smembers', setid)
        end
    end,
    --
    setadd = function(self, setid, score, id, autoincr)
        if autoincr then
            score = odm.redis.call('zincrby', setid, score, id)
        elseif self.meta.sorted then
            odm.redis.call('zadd', setid, score, id)
        else
            odm.redis.call('sadd', setid, id)
        end
        return score
    end,
    --
    setrem = function(self, setid, id)
        if self.meta.sorted then
            odm.redis.call('zrem', setid, id)
        else
            odm.redis.call('srem', setid, id)
        end
    end,
    --
    _queryset = function(self, tmp, field, field_type, setid)
        if field == self.meta.id_name then
            addset(setid, add)
        elseif field_type == 'u' then
            local mapkey, ids = self:mapkey(field), self:setids(setid)
            for _, v in ipairs(ids) do
                add(odm.redis.call('hget', mapkey, v))
            end
        elseif field_type == 'i' then
            addset(setid, union)
        else
            error('Cannot query on field "' .. field .. '". Not an index.')
        end 
    end,
    --
    _queryvalue = function(self, tmp, field, field_type, value)
        if field == self.meta.id_name then
            add(value)
        elseif field_type == 'u' then
            local mapkey = self:mapkey(field)
            add(odm.redis.call('hget', mapkey, value))
        elseif field_type == 'i' then
            union(value)
        else
            error('Cannot query on field "' .. field .. '". Not an index.')
        end
    end,
    --
    _selectranges = function(self, tmp, field, field_type, ranges)
        local ids = self:setids(tmp)
        for _, range in ipairs(ranges) do
            if field == self.meta.id_name then
                for _, id in ipairs(ids) do
                    if range.selector(id, r.v1, r.v2) then
                        all(id)
                    end
                end
            else
                for _, id in ipairs(ids) do
                    v = odm.redis.call('hget', self:object_key(id), field)
                    if range.selector(v, r.v1, r.v2) then
                        add(v)
                    end
                end
            end
        end
    end,
    --
    _commit_instance = function (self, action, id, score, data)
        local created_id, composite_id, errors = false, self.meta.id_type == COMPOSITE_ID, {}
        if self.meta.id_type == AUTO_ID then
            if id == '' then
                created_id = true
                id = odm.redis.call('incr', self.auto_ids)
            else
                id = id + 0 --  must be numeric
                local counter = odm.redis.call('get', self.auto_ids)
                if not counter or counter + 0 < id then
                    odm.redis.call('set', self.auto_ids, id)
                end
            end
        end
        if id == '' and not composite_id then
            table.insert(errors, 'Id not avaiable.')
        else
            local oldid, idkey, original_values = id, self:object_key(id), {}
            if action == 'override' or action == 'change' then  -- override or change
                original_values = odm.redis.call('hgetall', idkey)
                errors = self:update_indices(false, id, oldid, score)
                if action == 'override' then
                    odm.redis.call('del', idkey)
                end
            end
            -- Composite ID. Calculate new ID and data
            if composite_id then
                id = update_composite_id(original_values, data)
                idkey = self:object_key(id)
            end
            if id ~= oldid and oldid ~= '' then
                self:setrem(self.idset, oldid)
            end
            score = self:setadd(self.idset, score, id, self.meta.autoincr)
            if # data > 0 then
                odm.redis.call('hmset', idkey, unpack(data))
            end
            errors = self:update_indices(true, id, oldid, score)
            -- An error has occured. Rollback changes.
            if # errors > 0 then
                -- Remove indices
                self:update_indices('delete', id, oldid)
                if action == 'add' then
                    if created_id then
                        odm.redis.call('decr', self.auto_ids)
                        id = ''
                    end
                elseif # original_values > 0 then
                    id = oldid
                    idkey = self:object_key(id)
                    odm.redis.call('hmset', idkey, unpack(original_values))
                    self:update_indices(true, id, oldid, score)
                end
            end
        end
        if # errors > 0 then
            return {id, 0, errors[1]}
        else
            return {id, 1, score}
        end
    end,
    --
    update_indices = function (self, oper, id, oldid, score)
        local idkey, errors, idxkey = self:object_key(id), {}
        for field, unique in pairs(self.meta.indices) do
            -- obtain the field value
            local value = odm.redis.call('hget', idkey, field)
            if unique then
                idxkey = self:mapkey(field) -- id for the hash table mapping field value to instance ids
                if oper == 'delete' then
                    if value then
                        odm.redis.call('hdel', idxkey, value)
                    end
                elseif oper then
                    if odm.redis.call('hsetnx', idxkey, value, id) + 0 == 0 then
                        -- The value was already available!
                        if oldid == id or not odm.redis.call('hget', idxkey, value) == oldid then
                            -- remove the field from the instance hashtable so that
                            -- the next call to update_indices won't delete the index
                            -- odm.redis.call('hdel', idkey, field)
                            table.insert(errors, 'Unique constraint "' .. field .. '" violated: "' .. value .. '" is already in database.')
                        end
                    end
                end
            else
                idxkey = self:index_key(field, value)
                if oper == 'delete' then
                    self:setrem(idxkey, id)
                elseif oper then
                    self:setadd(idxkey, score, id)
                end
            end
        end
        if oper == 'delete' then
            local num = odm.redis.call('del', idkey) + 0
            self:setrem(self.idset, id)
            if self.meta.multifields then
                for _, name in ipairs(self.meta.multifields) do
                    odm.redis.call('del', idkey .. ':' .. name)
                end
            end
            return num
        else
            return errors
        end
    end,
    --
    -- Update a composite ID. Composite IDs are formed by two or more fields
    -- in an unique way.
    update_composite_id = function (self, original_values, data)
        local fields = {}
        local j = 0
        while j < # original_values do
            fields[original_values[j+1]] = original_values[j+2]
            j = j + 2
        end
        j = 0
        while j < # data do
            fields[data[j+1]] = data[j+2]
            j = j + 2
        end
        local newid = ''
        local joiner = ''
        for _,name in ipairs(composite_id) do
            newid = newid .. joiner .. name .. ':' .. fields[name]
            joiner = ','
        end
        return newid
    end
}
--
local model_meta = {}
-- Constructor
function odm.model(meta)
    local result = {}
    for k,v in pairs(odm.Model) do
        result[k] = v
    end
    result:init(meta)
    return setmetatable(result, model_meta)
end
-- Return the module only when this module is not in REDIS
if not redis then
    return odm
else
    odm.redis = redis
end
    