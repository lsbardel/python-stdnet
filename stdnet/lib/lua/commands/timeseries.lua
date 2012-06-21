-- UNIVARIATE TIMESERIES IN REDIS
--
-- Stand alone Lua script for managing an univariate timeseries in Redis
-- This script can be used by any clients, not just python-stdnet
local function single_id(ids, cname)
	if # ids == 1 then
		return ids[1]
	else
		error('Only one timeseries permitted for command ' .. cname)
	end
end

-- COMMANDS TABLE
local timeseries_commands = {
	size = function (self, ids)
		local id = single_id(ids, 'len')
		return redis.call('zcard', id)
	end,
	-- Add timestamp-value pairs to the timeseries
	add = function (self, ids, ...)
		local id = single_id(ids, 'add')
		local N = # arg
		local h = math.floor(N/2)
		assert(2*h == N, 'Cannot add to timeserie. Number of arguments must be even')
		local i = 0
		local timestamp, value, stimestamp
		while i < N do
			timestamp = arg[i+1] + 0
			value = cjson.encode({timestamp, arg[i+2]})
			redis.call('zremrangebyscore', id, timestamp, timestamp)
			redis.call('zadd', id, timestamp, value)
			i = i + 2
		end
	end,
	-- Check if *timestamp* exists in the timeseries
	exists = function (self, ids, timestamp)
		local value = self._single_value(ids, timestamp, 'exists')
		if value then
			return true
		else
			return false
		end
	end,
	-- Rank of *timestamp* in timeseries
	rank = function (self, ids, timestamp)
		local value = self._single_value(ids, timestamp, 'rank')
		if value then
			return redis.call('zrank', id, value)
		end
	end,
	--
	get = function (self, ids, timestamp)
		local value = self._single_value(ids, timestamp, 'get')
		if value then
			return cjson.decode(value)[2]
		end
	end,
	-- list of timestamps between *timestamp1* and *timestamp2*
	times = function (self, ids, timestamp1, timestamp2)
		local id = single_id(ids, 'times')
		local range = redis.call('zrangebyscore', id, timestamp1, timestamp2)
		return self._get_times(range)
	end,
	--
	itimes = function (self, ids, start, stop)
		local id = single_id(ids, 'itimes')
		local range = redis.call('zrange', id, start, stop)
		return self._get_times(range)
	end,
	--
	range = function (self, ids, timestamp1, timestamp2)
		local id = single_id(ids, 'range')
		local range = redis.call('zrangebyscore', id, timestamp1, timestamp2)
		return self._get_range(range)
	end,
	--
	irange = function (self, ids, start, stop)
		local id = single_id(ids, 'irange')
		local range = redis.call('zrange', id, start, stop)
		return self._get_range(range)
	end,
	--
	count = function (self, ids, timestamp1, timestamp2)
		local id = single_id(ids, 'count')
		return redis.call('zcount', id, timestamp1, timestamp2)
	end,
	--
	all_flat = function (self, ids)
		local id = single_id(ids, 'irange')
		local range = redis.call('zrange', id, 0, -1)
		local result = {}
		local v
		for i, value in ipairs(range) do
			v = cjson.decode(value)
			table.insert(result, v[1])
			table.insert(result, v[2])
		end
		return result
	end,
	--
	--	INTERNAL FUNCTIONS
	_single_value = function(ids, timestamp, name)
		local id = single_id(ids, name)
		local ra = redis.call('zrangebyscore', id, timestamp, timestamp)
		if # ra == 1 then
			return ra[1]
		elseif # ra > 1 then
			error('Critical error in timeserie. Multiple values for a timestamp')
		end
	end,
	--
	_get_times = function(range)
		local result = {}
		for i, value in ipairs(range) do
			table.insert(result, cjson.decode(value)[1])
		end
		return result
	end,
	--
	_get_range = function(range)
		local result = {}
		for i, value in ipairs(range) do
			table.insert(result, cjson.decode(value))
		end
		return result
	end,
}

local timeseries = {
	call = function(command, id, ...)
		local cmd = timeseries_commands[command]
		if not cmd then
			error('Timeserie command ' .. command .. ' not available')
		end
		return cmd(timeseries_commands, {id}, unpack(arg))
	end,
	--
	call_mult = function(command, ids, ...)
		local cmd = timeseries_commands[command]
		if not cmd then
			error('Timeserie command ' .. command .. ' not available')
		end
		return cmd(timeseries_commands, ids, unpack(arg))
	end
}