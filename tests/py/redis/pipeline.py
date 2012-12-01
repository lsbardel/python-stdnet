from .base import TestCase, redis


class PipelineTestCase(TestCase):
    
    def test_pipeline(self):
        pipe = self.client.pipeline()
        self.assertTrue(pipe.empty)
        self.assertRaises(redis.RedisError, pipe.add_callback, None)
        pipe.set('a', 'a1').get('a').zadd('z', 1, 'z1', -4, 'z2', 5, 'z3')\
                                    .zadd('z', -10, 'z4')
        pipe.zincrby('z', 'z1', 2).zrange('z', 0, 5, withscores=True)
        vals = pipe.execute()
        self.assertEqual(len(vals),6)
        vals[5] = list(vals[5])
        self.assertEquals(vals,
            [
                True,
                b'a1',
                True,
                True,
                3.0,
                [(b'z4', -10.0), (b'z2', -4.0), (b'z1', 3.0), (b'z3', 5.0)],
            ]
            )

    def test_invalid_command_in_pipeline(self):
        # all commands but the invalid one should be excuted correctly
        self.client['c'] = 'a'
        pipe = self.client.pipeline()
        pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
        result = pipe.execute()

        self.assertEquals(result[0], True)
        self.assertEquals(self.client['a'], b'1')
        self.assertEquals(result[1], True)
        self.assertEquals(self.client['b'], b'2')
        # we can't lpush to a key that's a string value, so this should
        # be a redis.RedisInvalidResponse exception
        self.assert_(isinstance(result[2], redis.RedisInvalidResponse))
        self.assertEquals(self.client['c'], b'a')
        self.assertEquals(result[3], True)
        self.assertEquals(self.client['d'], b'4')

        # make sure the pipe was restored to a working state
        self.assertEquals(pipe.set('z', 'zzz').execute(), [True])
        self.assertEquals(self.client['z'], b'zzz')

    def test_pipeline2(self):
        pipe = self.client.pipeline()
        pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
        self.assertEquals(pipe.execute(), [True, True, True])
        self.assertEquals(self.client['a'], b'a1')
        self.assertEquals(self.client['b'], b'b1')
        self.assertEquals(self.client['c'], b'c1')
        
    def test_pipeline_request(self):
        pipe = self.client.pipeline()
        pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
        request = pipe.request()
        self.assertTrue(request.is_pipeline)
        self.assertTrue(str(request).startswith('PIPELINE'))

