-- ENTRY FOR ALL COLUMNTS SCRIPTS
local scripts = {
	-- Run a user script agains the timeserie
	evaluate = function(series, script, ...)
		local context = {
			self = series[1],
			series = series,
			others = tabletools.slice(series, 2, -1)
		}
		local script_function = tabletools.load_code(script, context)
		return script_function()
	end,
	-- Merge timeseries
	merge = function(series, ...)
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
-- KEYS CONTRAIN TIMESERIES IDS
local series = {}
for i, id in ipairs(KEYS) do
	table.insert(series, columnts:new(id))
end
if # series == 0 then
    error('No timeseries available')
end
-- RUN THE SCRIPT
return script(series, unpack(tabletools.slice(ARGV, 2, -1)))
