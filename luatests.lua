-- TEST SUITE FOR LUA SCRIPTS
-- Requires lunatests from https://github.com/silentbicycle/lunatest
-- To run tests simply
--      lua luatests.lua
--
package.path = package.path .. ";stdnet/lib/lua/?.lua"
require("lunatest")
print '=============================='
print('To run just some tests, add "-t [pattern]"')
print '=============================='

lunatest.suite("tests/lua/utils")
lunatest.suite("tests/lua/redis_mock")
lunatest.suite("tests/lua/columnts")


lunatest.run()