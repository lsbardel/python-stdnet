-- IMPORTANT!
-- THIS MODULE IS NOT USED AS A SCRIPT FOR REDIS
--
-- Mock Redis lua scripting for stand-alone lua scripts.
-- This module is useful for testing redis lua scripts outside redis
--
-- To use it
-- redis = require("redis")
-- redis.call(...
--
local redis = {}
local store = {}
local bad_key = 'Redis Error: Operation performed against wrong type of key'

local hash_meta = {name = 'hash'}
local hash_list = {name = 'list'}

local function is_hash(h)
    if type(h) == 'table' then
        return getmetatable(h).name == 'hash'
    else
        return false
    end
end

local function is_list(h)
    if type(h) == 'table' then
        return getmetatable(h).name == 'list'
    else
        return false
    end
end

local function encode (v)
    if v then
        return v .. ''
    else
        return ''
    end
end

-- Utility for adding items to a list
local function add_to_list (key, value, values, callback)
    local key = encode(key)
    local h = store[key]
    if not h then
        h = setmetatable({}, hash_list)
        store[key] = h
    end
    if is_list(h) then
        if callback then
            callback(h,encode(value))
            for _,v in ipairs(values) do
                callback(h,encode(v))
            end
        end
        return # h
    else
        error(bad_key)
    end
end

-- REDIS COMMAND TABLE
redis.commands = {
    --
    -- KEYS COMMAND
    exists = function(key)
        return store[encode(key)] ~= nil
    end,
    keys = function()
        local all = {}
        for name,_ in pairs(store) do
            table.insert(all, name)
        end
        return all
    end,
    --
    -- STRING COMMANDS
    get = function (key)
        local v = store[encode(key)]
        if type(v) == 'table' then
            error(bad_key)
        else
            return v
        end
    end,
    set = function (key, value)
        store[encode(key)] = value
        return {ok='OK'}
    end,
    --
    -- LIST COMMANDS
    llen = function(key)
        return add_to_list(key)
    end,
    lpush = function(key, value, ...)
        return add_to_list(key, value, arg, function(h,v) table.insert(h,1,v) end)
    end,
    rpush = function(key, value, ...)
        return add_to_list(key, value, arg, function(h,v) table.insert(h,v) end)
    end,
    --
    -- HASH COMMANDS
    hset = function(key, field, value)
        local key = encode(key)
        local h = store[key]
        if not h then
            h = setmetatable({}, hash_meta)
            store[key] = h
        end
        if is_hash(h) then
            r = h[field] and 1 or 0
            h[field] = value
            return r
        else
            error(bad_key)
        end
    end,
    hget = function(key, field)
        return store[key][field]
    end,
    hgetall = function(key)
        return store[key]
    end,
    --
    -- SERVER COMMANDS
    flushall = function ()
        store = {}
    end
}

redis.call = function (command, ...)
    local com = redis.commands[string.lower(command)]
    if com then
        return com(unpack(arg))
    else
        error('Unknown redis command ' .. command)
    end
end

return redis