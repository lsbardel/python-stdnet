-- TEST SUITE FOR LUA SCRIPTS
-- Requires lunatests from https://github.com/silentbicycle/lunatest
-- To run tests simply
--      lua luatests.lua
--
package.path = package.path .. ";stdnet/lib/lua/?.lua;tests/lua/?.lua"
-- To run the debugger in eclipse you need to install the DBGp Client
-- http://wiki.eclipse.org/Koneki/LDT/User_Guide/Concepts/Debugger#Source_Mapping
-- Then create a lua project where to run the debug server
pcall(function() require("debugger")() end)

require("lunatest")
print '=============================='
print('To run just some tests, add "-t [pattern]"')
print '=============================='

lunatest.suite("tests/lua/utils")
lunatest.suite("tests/lua/odm")
lunatest.suite("tests/lua/columnts")


lunatest.run()