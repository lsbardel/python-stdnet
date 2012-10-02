-- MANAGE ALL COLUMNTS SCRIPTS called by stdnet
local scripts = {
    -- Commit a session to redis
    commit = function(self, model, keys, num, ...)
        return model:commit(num+0, arg)
    end,
    -- Build a query and store results on a new set. Returns the set id
    query = function(self, model, keys, field, ...)
        if # keys > 0 then
            return model:query(field, keys[1], arg)
        else
            error('Script query requires 1 key for the id set')
        end
    end,
    -- Load a query
    load = function(self, model, keys, options)
        if # keys > 0 then
            return model:load(keys[1], cjson.decode(options))
        else
            error('Script load requires 1 key for the id set')
        end
    end,
    -- delete a query
    delete = function(self, model, keys)
        if # keys > 0 then
            return model:delete(keys[1])
        else
            error('Script delete requires 1 key for the id set')
        end
    end
}


-- THE FIRST ARGUMENT IS THE NAME OF THE SCRIPT
if # ARGV < 2 then
    error('Wrong number of arguments.')
end
local script, meta = scripts[ARGV[1]], cjson.decode(ARGV[2]) 
if not script then
	error('Script ' .. ARGV[1] .. ' not available')
end
return script(scripts, odm.model(meta), KEYS, unpack(tabletools.slice(ARGV, 3, -1)))
