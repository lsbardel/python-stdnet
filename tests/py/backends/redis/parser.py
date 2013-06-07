from stdnet.utils import test
from stdnet.backends import redisb


lua_nested_table = '''
local nesting = ARGV[1]
local pres = {100, 'first string'}
local result = pres
for i=1,nesting do
    local res = {-8, 'a string'}
    pres[3] = res
    pres = res
end
return result
'''


class TestParser(test.TestCase):
    
    def parser(self):
        return redisb.PythonRedisParser()
    
    def test_status(self):
        parser = self.parser()
        parser.feed(b'+OK\r\n')
        response = parser.get()
        self.assertEqual(response, b'OK')
        self.assertEqual(parser.buffer(), b'')
        
    def test_string(self):
        parser = self.parser()
        parser.feed(b'$10\r\nciao\r\nluca\r\n+OK\r\n')
        response = parser.get()
        self.assertEqual(response, b'ciao\r\nluca')
        self.assertEqual(parser.buffer(), b'+OK\r\n')
        response = parser.get()
        self.assertEqual(response, b'OK')
        self.assertEqual(parser.buffer(), b'')
        
    def test_nested20(self):
        r = self.get_client()
        result = r.eval(lua_nested_table, 0, 20)
        self.assertTrue(result)
        
    def test_nested2(self):
        r = self.get_client()
        result = r.eval(lua_nested_table, 0, 2)
        

@test.skipUnless(redisb.HAS_C_EXTENSIONS, 'Requires C extensions')
class TestCParser(TestParser):
    
    def parser(self):
        return redisb.CppRedisParser()