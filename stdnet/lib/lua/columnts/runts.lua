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
	get = function(self, series, dte)
		local serie = self.on_serie_only(series, 'get')
		local fields = serie:get(dte, true)
		if fields then
			return tabletools.flat(fields)
		end
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
	range = function(self, series, start, stop, ...)
		local serie = self.on_serie_only(series, 'range')
		return self.flatten(serie:range(start, stop, arg, true))
	end,
	--
	irange = function(self, series, start, stop, ...)
		local serie = self.on_serie_only(series, 'irange')
		return self.flatten(serie:irange(start, stop, arg, true))
	end,
	--
	pop_range = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'pop_range')
		return self.flatten(serie:pop_range(start, stop, true))
	end,
	--
	ipop_range = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'ipop_range')
		return self.flatten(serie:ipop_range(start, stop, true))
	end,
	--
	stats = function(self, series, start, stop, ...)
		local fields = {arg}
		local sdata = self.stats_data(series, start, stop, fields, false)
		local serie = self.on_serie_only(sdata, 'stats')
		local result = statistics.univariate(serie)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	istats = function(self, series, start, stop, ...)
		local fields = {arg}
		local sdata = self.stats_data(series, start, stop, fields, true)
		local serie = self.on_serie_only(sdata, 'istats')
		local result = statistics.univariate(serie)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	multi_stats = function(self, series, start, stop, fields)
		fields = cjson.decode(fields)
		local sdata = self.stats_data(series, start, stop, fields, false)
		local result = statistics.multivariate(sdata)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	imulti_stats = function(self, series, start, stop, fields)
		fields = cjson.decode(fields)
		local sdata = self.stats_data(series, start, stop, fields, true)
		local result = statistics.multivariate(sdata)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	merge = function(self, series, data)
		local serie = self.on_serie_only(series, 'merge')
		serie:del()
		data = cjson.decode(data)
		local fields = data.fields
		local elements = data.series
    	-- First we copy the first timeseries across to self
    	for i, elem in ipairs(elements) do
        	assert( # elem.series <= 2, 'Too many timeseries. Cannot perform operation')
        	-- More than one timeseries. Create the timeseries obtain by multiplying them
        	local ts,tsmul = columnts:new(elem.series[1])
        	if # elem.series == 2 then
            	tsmul = ts
            	ts = columnts:new(elem.series[2])
        	end
        	serie:addserie(ts, elem.weight, fields, tsmul)
    	end
    	return serie:length()
	end,
	--
	session = function(self, series, ...)
		local serie = self.on_serie_only(series, 'session')
		local index = 1
		local num_remove = arg[index] + index
		while index < num_remove do
			index = index + 1
			serie:pop(arg[index])
		end
		index  = index + 1
		local num_strings = arg[index] + index
		while index < num_strings do
			local field = arg[index+1]
			index = index + 1
			serie:popfield(field)
		end
		while index < # arg do
			local field = arg[index+1]
			local times, values, field_values = {}, {}, {}
			field_values[field] = values
			index = index + 2
			local len_data = index + 2*arg[index]
			while index < len_data do
			    table.insert(times, arg[index+1] + 0)
			    table.insert(values, arg[index+2])
			    index = index + 2
			end
			serie:add(times, field_values)
		end
		return serie:length()
	end,
	--
	----------------------------------------------------------------------------
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
	end,
	--
	stats_data = function(series, start, stop, fields, byrank)
		sdata = {}
		local time_values
		for i, serie in ipairs(series) do
        	local fields = fields[i]
        	if byrank then
        		time_values = serie:irange(start, stop, fields)
        	else
        		time_values = serie:range(start, stop, fields)
        	end
        	local t,v = unpack(time_values)
        	table.insert(sdata, {key=serie.key, times=t, field_values=v})
    	end
    	return sdata
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
