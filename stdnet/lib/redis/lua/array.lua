-- An array of numbers for redis-lua

-- Not a number
local nan = 0/0
-- 8 bytes string for nil data
local nildata = string.char(0,0,0,0,0,0,0,0)

array = {
    --
    -- Initialize with key and optional initial size and value
    init = function (self, key, size, value)
        self.key = key
        self:allocate(size, value)
    end,
    -- length of array
    length = function (self)
        return (redis.call('strlen', self.key) + 0)/8
    end,
    -- Allocate extra size for the array
    allocate = function (self, size, value)
        if size then
            local length = self:length()
            if size > length then
                if value then
                    value = self:pack(value)
                else
                    value = nildata
                end
                value = string.rep(value,size-length) 
                redis.call('setrange', self.key, 8*length, value)
            end
        end
    end,
    --
    get = function(self, index)
        index = index + 0
        assert(index > 0 and index <= self:length(),"Out of bound.")
        local start = 8*(index - 1)
        return self:unpack(redis.call('getrange', self.key, start, start+8))
    end,
    set = function(self, index, value)
        index = index + 0
        assert(index > 0 and index <= self:length(),"Out of bound.")
        local start = 8*(index - 1)
        value = self:pack(value)
        return self:unpack(redis.call('setrange', self.key, start, value))
    end,
    --
    -- Internal functions
    pack = function(self, value)
        return pack('>d',value)
    end
    unpack = function(self, value)
        return unpack('>d',value)
    end
}


columnts_meta = {
    __index = function(self,index)
        return self:get(index)
    end
    __newindex = function(self,index)
        return self:set(index,value)
    end
}
-- Constructor
function array:new(key)
    local result = {}
    for k,v in pairs(columnts) do
        result[k] = v
    end
    result:init(key)
    return setmetatable(result, columnts_meta)
end
