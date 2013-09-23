
local ts = columnts:new(KEYS[1])

assert(ts.key == KEYS[1])
assert(ts:fieldkey('myfield') == KEYS[1] .. ':field:myfield')
assert(# ts:fields() == 0)

return {ok = 'OK'}
