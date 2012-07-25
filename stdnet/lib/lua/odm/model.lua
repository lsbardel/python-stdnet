local ModelMeta = {
    auto_id = true,
    composite_id = false,
    indices = {},
    uniques = {}
}

-- Model pseudo-class
local Model = {
    --
    --[[
     Initialize model with:
        bk: the model base key (prefix to all model keys)
        options: table containing the model metadata.
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
        local instance = {id=id, errors={}}
        self:createid(instance);
        if # instance.errors == 0 then
            self.index(instance)
        end
    end,
    ---------------------------------------------------------------
    --  INTERNALS
    --
    -- Create the instance id if needed
    createid = function(self, instance)
        instance.created_id = false
        instance.oldid = instance.id
        if self.options.auto_id then
            if instance.id == '' then
                instance.created_id = true
                instance.id = redis.call('incr', self.auto_ids) + 0
            else
                instance.id = instance.id + 0  -- must be numeric
                local counter = redis.call('get', self.auto_ids)
                if not counter or counter + 0 < id then
                    redis.call('set', self.auto_ids, id)
                end
            end
        end
        if instance.id == '' and not self.meta.composite_id then
            table.insert(instance.errors, 'Id not avaiable.')
        end
    end,
    --
    index = function (instance)
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