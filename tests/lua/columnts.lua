-- tests stdnet.apps.columnts.lua internal functions
local t = require("tabletools")
local stats = require("columnts/stats")

local function series_for_test ()
    -- TEST MULTI SERIES AGGREGATION
    local serie1 = {key='first',
                    times={1001,1002,1003,1004},
                    field_values={field1 = {1,3,2,4}}}
    local serie2 = {key='second',
                    times={1001,1002,1004,1005,1006},
                    field_values={field1 = {1,1.5,2,4,1.5}}}
    local serie3 = {key='third',
                    times={1001,1002,1003,1004,1009},
                    field_values={field1 = {-2,3,2.4,4,6.4}}}
    
    return {serie1, serie2, serie3}
end


local suite = {}


suite.test_vector_sadd = function ()
    local v1 = t.init(5,3)
    local v2 = t.init(5,-2)
    local v3 = stats.vector_sadd(v1,v2)
    assert_equal(# v1, 5)
    assert_true(t.equal(v1,v3))
    assert_true(t.equal(v1,{1,1,1,1,1}))
    assert_true(t.equal(v2,{-2,-2,-2,-2,-2}))
    
    v1 = {1,2,3}
    stats.vector_sadd(v1, {3,-1,6})
    assert_true(t.equal(v1, {4,1,9}))
end


suite.test_vector_diff = function ()
    local v1 = t.init(5,3)
    local v2 = t.init(5,-2)
    local v3 = stats.vector_diff(v1,v2)
    assert_equal(# v3, 5)
    assert_false(t.equal(v1,v3))
    assert_true(t.equal(v1,{3,3,3,3,3}))
    assert_true(t.equal(v2,{-2,-2,-2,-2,-2}))
    assert_true(t.equal(v3,{5,5,5,5,5}))
end


suite.test_square = function()
    local vector = t.init(7,98)
    local vector2 = stats.vector_square(vector)
    assert_equal(# vector2, 28)
    for i,v in ipairs(vector2) do
        assert_equal(v, 98*98)
    end

    vector = {2,4,9,-2}
    vector2 = stats.vector_square(vector)
    assert_equal(# vector2, 10)
    assert_equal(vector2[1], 4)
    assert_equal(vector2[2], 8)
    assert_equal(vector2[3], 16)
    assert_equal(vector2[4], 18)
    assert_equal(vector2[5], 36)
    assert_equal(vector2[6], 81)
    assert_equal(vector2[7], -4)
    assert_equal(vector2[8], -8)
    assert_equal(vector2[9], -18)
    assert_equal(vector2[10], 4)
end


suite.test_series = function()
    local series = series_for_test()
    local a = stats.fields_and_times(series)
    assert_equal(# a.times, 4)
    assert_true(t.equal(a.times, series[1].times))
    assert_equal(# a.names, 3)
    assert_true(t.equal(a.names, {'first @ field1','second @ field1','third @ field1'}))
    assert_equal(# a.time_dict, 0)
    local flat = t.flat(a.time_dict)
    assert_equal(# flat, 8)
    assert_equal('string', type(flat[1]))
    assert_equal('string', type(flat[3]))
    assert_equal('string', type(flat[5]))
    assert_equal('string', type(flat[7]))
    local ofield = {flat[1]+0,flat[3]+0,flat[5]+0,flat[7]+0}
    table.sort(ofield)
    assert_true(t.equal(ofield,{1001,1002,1003,1004}))
    assert_true(t.equal({1,1,-2}, a.time_dict['1001']))
    assert_true(t.equal({3,1.5,3}, a.time_dict['1002']))
    assert_true(t.equal({2, 2.4}, a.time_dict['1003']))
    assert_true(t.equal({4,2,4}, a.time_dict['1004']))
end


suite.test_multivariate = function()
    local series = series_for_test()
    local result = stats.multivariate(series)
    assert_equal(3, result.N)
    assert_true(t.equal({'first @ field1','second @ field1','third @ field1'}, result.fields))
    assert_true(t.equal({8,4.5,5}, result.sum))
    assert_true(t.equal({26,13.5,7.25,23,10.5,29}, result.sum2))
    assert_true(t.equal({3,1,6}, result.dsum))
    assert_true(t.equal({5,1.5,0.5,11,3,26}, result.dsum2))
end

return suite

