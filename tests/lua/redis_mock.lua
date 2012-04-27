local redis = require("redis")

local suite = {}

local function clear ()
    redis.call('flushall')
    assert_true(# redis.call('keys') == 0)
end

suite.test_flushall = function()
    clear()
end

suite.test_set_get_exists = function()
    clear()
    assert_false(redis.call('exists','foo'))
    assert_equal(redis.call('set','foo','bla')['ok'],'OK')
    assert_true(redis.call('exists','foo'))
    assert_false(redis.call('exists','fooo'))
    assert_equal(redis.call('get','foo'), 'bla')
end


suite.test_list = function()
    clear()
    assert_false(redis.call('exists','foo'))
    assert_equal(1, redis.call('lpush','foo','ciao'))
    assert_equal(2, redis.call('rpush','foo','luca'))
    assert_equal(2, redis.call('llen', 'foo'))
end


return suite