-- MANAGE ALL COLUMNTS SCRIPTS called by stdnet
local scripts = {
	-- Run a user script agains the timeserie
	evaluate = function(self, series, script, ...)
		local context = {
			self = series[1],
			series = series,
			others = tabletools.slice(series, 2, -1)
		}
		local script_function = tabletools.load_code(script, context)
		return script_function()
	end,
	-- Merge timeseries
	merge = function(self, series, ...)
	end,
	--
	times = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'times')
		return serie:times(start, stop)
	end,
	--
	itimes = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'itimes')
		return serie:itimes(start, stop)
	end,
	--
	pop_range = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'pop_range')
		return self.flatten(serie:pop_range(start, stop, false))
	end,
	--
	ipop_range = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'ipop_range')
		return self.flatten(serie:ipop_range(start, stop, false))
	end,
	--
	-- INTERNALS
	--
	on_serie_only = function(series, name)
		if # series > 1 then
			error(name .. ' requires only one time series.')
		end
		return series[1]
	end,
	--
	flatten = function(time_values)
		local result = {time_values[1]}
    	for k, v in pairs(time_values[2]) do
        	table.insert(result, k)
        	table.insert(result, v)	
    	end
    	return result
	end
}

-- THE FIRST ARGUMENT IS THE NAME OF THE SCRIPT
if # ARGV == 0 then
	error('The first argument must be the name of the script. Got nothing.')
end
local script = scripts[ARGV[1]]
if not script then
	error('Script ' .. ARGV[1] .. ' not available')
end
-- KEYS CONTAIN TIMESERIES IDS
local series = {}
for i, id in ipairs(KEYS) do
	table.insert(series, columnts:new(id))
end
if # series == 0 then
    error('No timeseries available')
end
-- RUN THE SCRIPT
return script(scripts, series, unpack(tabletools.slice(ARGV, 2, -1)))
