local t = require("tabletools")

local suite = {}

suite.test_init = function ()
    assert_true(# t.init(5,-3) == 5)
    assert_true(# t.init(0,7) == 0)
    assert_true(# t.init(-1,0) == 0)
end


suite.test_init_value = function ()
    local vector = t.init(7, 98)
    assert_true(# vector == 7)
    for i,v in ipairs(vector) do
        assert(v == 98)
    end
end



return suite
