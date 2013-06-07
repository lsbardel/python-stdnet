from stdnet.utils import test
from stdnet.backends.redisb import RedisParser


lua_nested_table = '''
local s = ''
for i=1,100 do
    s = s .. '1234567890'
end
local nesting = ARGV[1]
local pres = {100, s}
local result = pres
for i=1,nesting do
    local res = {-8, s}
    pres[3] = res
    pres[4] = res
    pres = res
end
return result
'''


class TestParser(test.TestCase):
    
    @classmethod
    def after_setup(cls):
        cls.client = cls.backend.client

    def test_null(self):
        test = b'$-1\r\n'
        p = RedisParser()
        p.feed(test)
        self.assertEqual(p.get(), None)
        
    def __test_multi(self):
        test = b'+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n$-1\r\n:1\r\n:39\r\n'
        p = RedisParser()
        p.feed(test)
        self.assertEqual(p.get(), b'OK')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), [None, 1, 39])
        
    def __test_nested10(self):
        result = self.client.eval(lua_nested_table, 0, 10)
        self.assertEqual(len(result), 4)
        
    def __test_nested2(self):
        result = self.client.eval(lua_nested_table, 0, 2)
        self.assertEqual(len(result), 4)
        
