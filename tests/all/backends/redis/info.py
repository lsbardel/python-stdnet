import time

from stdnet.backends.redisb import RedisDataFormatter, RedisDb, RedisKey

from . import client


class TestInfo(client.TestCase):
    models = (RedisDb, RedisKey)

    def get_manager(self, key="test", value="bla"):
        yield self.client.set(key, value)
        yield self.mapper.redisdb

    def test_dataFormatter(self):
        f = RedisDataFormatter()
        self.assertEqual(f.format_date("bla"), "")
        d = f.format_date(time.time())
        self.assertTrue(d)

    def testKeyInfo(self):
        yield self.client.set("planet", "mars")
        yield self.client.lpush("foo", 1, 2, 3, 4, 5)
        yield self.client.lpush("bla", 4, 5, 6, 7, 8)
        keys = yield self.client.execute_script("keyinfo", (), "*")
        self.assertEqual(len(keys), 3)
        d = dict(((k.key, k) for k in keys))
        self.assertEqual(d["planet"].length, 4)
        self.assertEqual(d["planet"].type, "string")
        self.assertEqual(d["planet"].encoding, "raw")

    def testKeyInfo2(self):
        client = self.client
        yield self.multi_async(
            (
                client.set("planet", "mars"),
                client.lpush("foo", 1, 2, 3, 4, 5),
                client.lpush("bla", 4, 5, 6, 7, 8),
            )
        )
        keys = yield client.execute_script("keyinfo", ("planet", "bla"))
        self.assertEqual(len(keys), 2)

    def test_manager(self):
        redisdb = yield self.get_manager()
        self.assertTrue(redisdb.client)
        self.assertEqual(redisdb.backend.client, redisdb.client)
        self.assertTrue(redisdb.formatter)
        self.assertEqual(redisdb.formatter.format_name("ciao"), "ciao")
        self.assertEqual(redisdb.formatter.format_bool(0), "no")
        self.assertEqual(redisdb.formatter.format_bool("bla"), "yes")

    def test_info_pannel_names(self):
        info = yield self.client.info()
        self.assertTrue(info)
        for name in self.mapper.redisdb.names:
            self.assertTrue(name in info)

    def test_databases(self):
        redisdb = yield self.get_manager()
        dbs = yield redisdb.all()
        self.assertTrue(dbs)
        self.assertIsInstance(dbs, list)
        for db in dbs:
            self.assertIsInstance(db.db, int)
            self.assertTrue(db.expires <= db.keys)

    def test_makepanel_empty(self):
        redisdb = yield self.get_manager()
        p = redisdb.makepanel("sdkjcbnskbcd", {})
        self.assertEqual(p, None)

    def test_panels(self):
        redisdb = yield self.get_manager()
        p = yield redisdb.panels()
        self.assertTrue(p)
        for name in redisdb.names:
            val = p.pop(name)
            self.assertIsInstance(val, list)
        self.assertFalse(p)

    def __test_database(self):
        redisdb = yield self.get_manager()
        dbs = yield redisdb.all()
        for db in dbs:
            dbkeys = yield db.all_keys.all()
            self.assertIsInstance(dbkeys, list)

    def __testInfoKeys(self):
        redisdb = yield self.get_manager()
        dbs = RedisDb.objects.all(info)
        for db in dbs:
            keys = RedisKey.objects.query(db)
            self.assertEqual(keys.db, db)
            self.assertEqual(keys.pattern, "*")

    def __test_search(self):
        redisdb = yield self.get_manager()
        yield redisdb.client.set("blaxxx", "test")
        query = db.query()
        q = query.search("blax*")
        self.assertNotEqual(query, q)
        self.assertEqual(q.db, db)
        self.assertEqual(q.pattern, "blax*")
        self.assertTrue(query.count())
        self.assertEqual(query.count(), len(query))
        self.assertEqual(q.count(), 1)
        keys = list(q)
        self.assertEqual(len(keys), 1)
        key = q[0]
        self.assertEqual(str(key), "blaxxx")

    def __testQuerySlice(self):
        redisdb = yield self.get_manager()
        db = self.newdb(info)
        db.client.set("blaxxx", "test")
        db.client.set("blaxyy", "test2")
        all = db.all()
        self.assertTrue(isinstance(all, list))
        self.assertEqual(len(all), 2)
        self.assertEqual(all, db.query()[:])
        self.assertEqual(all[-1:], db.query()[-1:])
        self.assertEqual(all[-1:1], db.query()[-1:1])
        self.assertEqual(all[-1:2], db.query()[-1:2])
        self.assertEqual(all[-2:1], db.query()[-2:1])
        self.assertEqual(db.query().search("*yy").delete(), 1)
        self.assertEqual(db.query().delete(), 1)
        self.assertEqual(db.all(), [])

    def __testRedisKeyManager(self):
        redisdb = yield self.get_manager()
        db = self.newdb(info)
        db.client.set("blaxxx", "test")
        db.client.set("blaxyy", "test2")
        all = db.all()
        self.assertEqual(len(all), 2)
        self.assertEqual(RedisKey.objects.delete(all), 2)
        self.assertEqual(db.all(), [])

    def __testRedisDbDelete(self):
        redisdb = yield self.get_manager()
        dbs = RedisDb.objects.all(info)
        called = []
        flushdb = lambda client: called.append(client)
        for db in dbs:
            db.delete(flushdb)
        self.assertEqual(len(called), len(dbs))
