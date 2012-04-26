-- tests stdnet.apps.columnts.lua internal functions
package.path = package.path .. ";../../stdnet/apps/columnts/lua/?.lua"
require("stats")

assert(# init_vector(5,-3) == 5)
assert(# init_vector(0,7) == 0)
assert(# init_vector(-1,0) == 0)

local vector = init_vector(7,98)
for i,v in ipairs(vector) do
    assert(v == 98)
end

local vector2 = vector_square(vector)
assert(# vector2 == 28)
for i,v in ipairs(vector2) do
    assert(v == 98*98)
end

-- _VECTOR_SADD
sv = {1,2,3}
vector_sadd(sv, {3,-1,6})
assert(equal_vectors(sv,{4,1,9}))

-- SQUARE VECTOR
vector = {2,4,9,-2}
vector2 = vector_square(vector)
assert(# vector2 == 10)
assert(vector2[1] == 4)
assert(vector2[2] == 8)
assert(vector2[3] == 16)
assert(vector2[4] == 18)
assert(vector2[5] == 36)
assert(vector2[6] == 81)
assert(vector2[7] == -4)
assert(vector2[8] == -8)
assert(vector2[9] == -18)
assert(vector2[10] == 4)

-- TEST MULTI SERIES AGGREGATION
local serie1 = {key='first',
                times={1001,1002,1003,1004},
                field_values={field1 = {1,3,2,4}}}
local serie2 = {key='second',
                times={1001,1002,1004,1005,1006},
                field_values={field1 = {1,3,2,4,1.5}}}
local serie3 = {key='third',
                times={1001,1002,1003,1004,1009},
                field_values={field1 = {-2,3,2,4,6.4}}}

local series = {serie1, serie2, serie3}
local a = fields_and_times(series)
assert(# a.times == 4)
assert(equal_vectors(a.times, serie1.times))
assert(# a.names == 3)

local stats = multi_stats({serie1,serie2,serie3})

print 'OK'