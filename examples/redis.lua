-- A Stand-alone REDIS server implementation for testing redis lua scripts
-- outside redis.
--
-- To use it
-- redis = require("redis")
-- redis.call(...
--
local NUM_DATABASES = 16
local database = 1
local store = {}
local redis = {
    bad_key = 'Redis Error: Operation performed against wrong type of key',
    ok = {ok='OK'}
}

local flush_all = function ()
    store = {}
    for i=1, NUM_DATABASES do
        store[i] = {}
    end
end
flush_all()

local function encode (v)
    if v then
        return v .. ''
    else
        return ''
    end
end

local function noencode(v)
    return v
end

local function as_pairs(arg)
    local ps, c, v0 = {}
    for i, v in ipairs(arg) do
        if 2*math.floor(i/2) == i then
            table.insert(ps, {v0, v})
        else
            v0 = v
        end
    end
    return ps
end

local meta = {
    set = {name = 'set', encode = encode},
    list = {name = 'list', encode = encode},
    zset = {name = 'zset', encode = noencode},
    hash = {name = 'hash', encode = noencode}
}

local function is(h, name)
    return type(h) == 'table' and getmetatable(h).name == name
end

-- Utility for adding items to a list
local function oper (type, key, callback, value, values)
    local key = encode(key)
    local h = store[database][key]
    if not h then
        h = setmetatable({}, meta[type])
    end
    if is(h, type) then
        if callback then
            local vencode, cbk = getmetatable(h).encode
            store[database][key] = h
            value = vencode(value)
            cbk = callback(h, value)
            if cbk ~= nil then
                return cbk
            elseif values then
                for _, v in ipairs(values) do
                    callback(h, vencode(v))
                end
            end
        end
        if type == 'list' then
            return # h
        else
            local count = 0
            for _,_ in pairs(h) do
                count = count + 1
            end
            return count
        end
    else
        error(redis.bad_key)
    end
end

--------------------------------------------------------------------------------
-- REDIS COMMAND TABLE
--------------------------------------------------------------------------------
redis.commands = {
    ----------------------------------------------------------------------------
    -- KEY COMMANDS
    del = function (...)
        local count = 0
        for _, key in ipairs(arg) do
            key = encode(key)
            if store[database][key] ~= nil then
                store[database][key] = nil
                count = count + 1
            end
        end
        return count
    end,
    exists = function(key)
        return store[database][encode(key)] ~= nil
    end,
    keys = function()
        local all = {}
        for name,_ in pairs(store[database]) do
            table.insert(all, name)
        end
        return all
    end,
    ----------------------------------------------------------------------------
    -- STRING COMMANDS
    get = function (key)
        local v = store[database][encode(key)]
        if type(v) == 'table' then
            error(redis.bad_key)
        else
            return v
        end
    end,
    set = function (key, value)
        store[database][encode(key)] = value
        return redis.ok
    end,
    decr = function (key)
        return redis.commands.incrby(key, -1)
    end,
    decrby = function (key, decrement)
        return redis.commands.incrby(key, -decrement)
    end,
    incr = function (key)
        return redis.commands.incrby(key, 1)
    end,
    incrby = function (key, increment)
        local v = redis.commands.get(key)
        if v then
            v = v + increment
        else
            v = increment
        end
        redis.commands.set(key, v)
        return v
    end,
    ----------------------------------------------------------------------------
    -- LIST COMMANDS
    llen = function(key)
        return oper('list', key)
    end,
    lpush = function(key, value, ...)
        return oper('list', key, function(h,v) table.insert(h,1,v) end, value, arg)
    end,
    rpush = function(key, value, ...)
        return oper('list', key, function(h,v) table.insert(h,v) end, value, arg)
    end,
    ----------------------------------------------------------------------------
    -- SET COMMANDS
    sadd = function(key, value, ...)
        local c = 0
        oper('set', key, function(h, fi)
                fi = encode(fi)
                if h[fi] == nil then
                    c = c + 1
                    h[fi] = 1
                end
            end, value, arg)
        return c
    end,
    scard = function(key)
        return oper('set', key)
    end,
    srem = function(key, value, ...)
        local c = 0
        oper('set', key, function (h, fi)
                fi = encode(fi)
                if h[fi] ~= nil then
                    c = c + 1
                    h[fi] = nil
                end
            end, value, arg)
        return c
    end,
    ----------------------------------------------------------------------------
    -- HASH COMMANDS
    hdel = function(key, field, ...)
        local c = 0
        oper('hash', key, function (h, fi)
                fi = encode(fi)
                if h[fi] ~= nil then
                    c = c + 1
                    h[fi] = nil
                end
            end, field, arg)
        return c
    end,
    hexists = function(key, field)
        return oper('hash', key, function (h, field)
                return h[field] ~= nil
            end, field) 
    end,
    hget = function(key, field)
        return store[database][key][field]
    end,
    hgetall = function(key)
        return store[database][key]
    end,
    hset = function(key, field, value)
        return redis.commands.hmset(key, field, value)
    end,
    hmset = function(key, field, value, ...)
        local field_values = as_pairs(arg)
        local function _insert(h, field_value)
            local field, value = unpack(field_value)
            h[encode(field)] = encode(value)
        end
        return oper('hash', key, _insert, {field, value}, field_values)
    end,
    hset = function(key, field, value)
        return redis.commands.hmset(key, field, value)
    end,
    hsetnx = function(key, field, value)
        if redis.commands.hexists(key, field) == false then
            redis.commands.hset(key, field, value)
            return 1
        else
            return 0
        end
    end,
    ----------------------------------------------------------------------------
    -- SERVER COMMANDS
    dbsize = function ()
        local count = 0
        for _,v in pairs(store[database]) do
            count = count + 1
        end
        return count
    end,
    flushall = function ()
        flush_all()
        return redis.ok
    end,
    flushdb = function ()
        store[database] = {}
        return redis.ok
    end,
    select = function (db)
        if db >= 0 and db < NUM_DATABASES then
            database = db+1
        end
        return redis.ok
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