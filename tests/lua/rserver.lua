-- Obtain the redis client
local client = require("redis")
local redis = {
    hsetnx = function (result)
        if result then
            return 1
        else
            return 0
        end
    end
}

redis.connect = function (...)
    redis.client = client.connect(unpack(arg))
end

redis.call = function (command, ...)
    local lcom = string.lower(command)
    local com = redis.client[lcom]
    if com then
        local callback = redis[lcom]
        local res = com(redis.client, unpack(arg))
        if callback then
            res = callback(res)
        end
        return res
    else
        error('Unknown redis command ' .. command)
    end
end

return redis