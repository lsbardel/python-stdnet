'''Benchmark manipulation of keys in lua.'''
from stdnet import test

from examples.data import key_data

onebyone = '''
local prefix = ARGV[1]
local hash = ARGV[2] + 0
local keys = redis.call('keys', prefix .. '*')
local hkey = prefix .. 'hash-onebyone-test'
redis.call('del',hkey)

if hash == 1 then
    for _,key in ipairs(keys) do
        redis.call('hset', hkey, key, 'llllllllllllllllllllllllllllllllllll')
    end
else
    for _,key in ipairs(keys) do
        redis.call('set', key, 'llllllllllllllllllllllllllllllllllll')
    end
end
redis.call('del',hkey)
return table.getn(keys)
'''


unpack = '''
local limit = 3000
local prefix = ARGV[1]
local hash = ARGV[2] + 0
local keys = redis.call('keys', prefix .. '*')
local mapping = {}
local hkey = prefix .. 'hash-unpack-test'
redis.call('del',hkey)

local function mset()
    if hash == 1 then
        redis.call('hmset', hkey, unpack(mapping))
    else
        redis.call('mset', unpack(mapping))
    end
    mapping = {}
end

local j = 0
for _,key in ipairs(keys) do
    if j > limit then
        mset()
        j = 0
    end
    mapping[j+1] = 'key'
    mapping[j+2] = 'llllllllllllllllllllllllllllllllllll'
    j = j + 2
end

if table.getn(mapping) > 0 then
    mset()
end

redis.call('del',hkey)
return table.getn(keys)
'''

class ScriptingKeyTest(test.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.data = key_data(cls.size)
        
    def setUp(self):
        client = self.backend.client
        mapping = self.data.mapping(self.backend.namespace)
        client.mset(mapping)
        
    def testOneByOne(self):
        n = self.backend.client.eval(onebyone, (), self.backend.namespace, 1)
        self.assertEqual(n, self.data.size)
        
    def testUnpack(self):
        n = self.backend.client.eval(unpack, (), self.backend.namespace, 0)
        self.assertEqual(n, self.data.size)
        
    def testHashOneByOne(self):
        n = self.backend.client.eval(onebyone, (), self.backend.namespace, 1)
        self.assertEqual(n, self.data.size)
        
    def testHashUnpack(self):
        n = self.backend.client.eval(unpack, (), self.backend.namespace, 1)
        self.assertEqual(n, self.data.size)
        
        