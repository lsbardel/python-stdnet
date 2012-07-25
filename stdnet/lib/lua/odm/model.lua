local ModelMeta = {
    auto_id = true,
    composite_id = false,
    indices = {},
    uniques = {}
}

local Instance = {
    init = function (self, model, id, score)
        self.id = id .. ''
        self.score = score
        self.errors = {}
        self.model = model
        self.created_id = false
        self.oldid = instance.id
        if self.model.meta.auto_id then
            if self.id == '' then
                self.created_id = true
                self.id = redis.call('incr', self.model.auto_ids) + 0
            else
                self.id = self.id + 0  -- must be numeric
                local counter = redis.call('get', self.model.auto_ids)
                if not counter or counter + 0 < id then
                    redis.call('set', self.model.auto_ids, id)
                end
            end
        end
        if self.id == '' and not self.model.meta.composite_id then
            table.insert(self.errors, 'Id not avaiable.')
        end
    end,
    --
    start_transaction = function (self, action)
        local idkey = self.model.object_key(self.oldid)
        instance.original_values = nil
        if action == 'o' or action == 'c' then  -- override or change
            instance.original_values = redis.call('hgetall', idkey)
            update_indices(score, id, idkey, oldid, false)
            -- complete override, delete the original object
            if action == 'o' then
                redis.call('del', idkey)
            end
        end
    end
}

-- Model pseudo-class
local Model = {
    --
    --[[
     Initialize model with:
        bk: the model base key (prefix to all model keys)
        meta: table containing the model metadata.
    --]]
    init = function (self, bk, meta)
        self.bk = bk
        self.idset = bk .. ':id'    -- key for set containing all ids
        self.auto_ids = bk .. ':ids'
        self.meta = meta
    end,
    --[[
     Commit an instance to redis
        action: either 'o' for complete override or 'c' for change
        id: instance id, if not provided this is a new instance.
    --]]
    commit = function (self, action, id)
        local instance = Instance:get(self, id)
        if # instance.errors == 0 then
            instance:start_transaction(action)
        end
    end,
    --
    index = function (instance)
    end,
    --
    object_key = function (id)
        return self.bk .. ':obj:' .. id
    end
}


local model_meta = {}
-- Constructor
function Model:get(bk, indices, uniques)
    local result = {}
    for k,v in pairs(Model) do
        result[k] = v
    end
    result:init(key)
    return setmetatable(result, model_meta)
end
-- Instance Constructor
function Instance:get(model, id)
    local result = {}
    for k,v in pairs(Instance) do
        result[k] = v
    end
    result:init(model, id)
    return result
end
