local t = require("tabletools")
local odm = require("odm")

local suite = {}

suite.test_range_selectors = function ()
    assert_true(odm.range_selectors['ge'](3, 2))
    assert_true(odm.range_selectors['ge'](3, 3))
    assert_false(odm.range_selectors['ge'](3, 4))
end


return suite
