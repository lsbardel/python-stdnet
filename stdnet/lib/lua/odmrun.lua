-- MANAGE ALL COLUMNTS SCRIPTS called by stdnet
local scripts = {
    -- Commit a session to redis
    commit = function(self, model, num, ...)
        return model:commit(num, arg)
    end,
    -- Build a query and store results on a new set. Returns the set id
    query = function(self, model, field, ...)
        return model:query(field, arg)
    end,
    -- Load a query
    load = function(self, model, key)
        return model:load(key)
    end,
    -- delete a query
    delete = function(self, model, key)
        return model:delete(key)
    end
}


-- THE FIRST ARGUMENT IS THE NAME OF THE SCRIPT
if # ARGV < 2 then
    error('Wrong number of arguments.')
end
local script, meta = scripts[ARGV[1]], ARGV[2] 
if not script then
	error('Script ' .. ARGV[1] .. ' not available')
end
local model = odm.model(cjson.decode(meta))
return script(scripts, model, unpack(tabletools.slice(ARGV, 4, -1)))
