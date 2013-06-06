from stdnet.utils import test
from stdnet.backends import redisb


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
        

@test.skipUnless(redisb.HAS_C_EXTENSIONS, 'Requires C extensions')
class TestCParser(TestParser):
    
    def parser(self):
        return redisb.CppRedisParser()