import time

from stdnet.lib.redis import redis_info, RedisDb, RedisKey, RedisDataFormatter

from .base import TestCase


class TestInfo(TestCase):
    
    def setUp(self):
        super(TestInfo,self).setUp()
        self.client.set('test', 'bla')
        self.db = self.client.db
        self.info = redis_info(self.client)
        
    def newdb(self):
        db = self.info.databases[0]
        self.assertNotEqual(db.client, self.info.client)
        # make sure the database is clean
        db.client.flushdb()
        return db
        
    def testSimple(self):
        info = self.info
        self.assertTrue(info.client)
        self.assertEqual(info.client.db,self.db)
        self.assertTrue(info.version)
        self.assertTrue(info.formatter)
        self.assertEqual(info.formatter.format_name('ciao'),'ciao')
        self.assertEqual(info.formatter.format_bool(0),'no')
        self.assertEqual(info.formatter.format_bool('bla'),'yes')
        
    def testKeys(self):
        info = self.info
        dbs = info.databases
        self.assertTrue(dbs)
        self.assertTrue(isinstance(dbs,list))
        for db in dbs:
            self.assertEqual(db.id,db.client.db)
            self.assertEqual(len(info.db(db.id)),2)
            self.assertEqual(len(info.db(db.db)),2)
        
    def test_makepanel(self):
        info = self.info
        p = info.makepanel('sdkjcbnskbcd')
        self.assertEqual(p,None)
        for name in info.info:
            if not name:
                continue
            p = info.makepanel(name)
            self.assertTrue(p)
            self.assertTrue(isinstance(p,list))
            
    def test_panels(self):
        p = self.info.panels()
        self.assertTrue(p)
    
    def testRedisDBModel(self):
        info = self.info
        dbs = RedisDb.objects.all(info)
        self.assertTrue(dbs)
        self.assertTrue(isinstance(dbs,list))
        for db in dbs:
            c = RedisDb.objects.get(db = db.id, info = info)
            self.assertEqual(c.id,db.id)
        
    def testRedisDbDelete(self):
        info = self.info
        dbs = RedisDb.objects.all(info)
        called = []
        flushdb = lambda client: called.append(client)
        for db in dbs:
            db.delete(flushdb)
        self.assertEqual(len(called),len(dbs))
        
    def testInfoKeys(self):
        info = self.info
        dbs = RedisDb.objects.all(info)
        for db in dbs:
            keys = RedisKey.objects.query(db)
            self.assertEqual(keys.db, db)
            self.assertEqual(keys.pattern, '*')
            
    def testQuery(self):
        db = self.newdb()
        db.client.set('blaxxx', 'test')
        query = db.query()
        self.assertEqual(query.db, db)
        q = query.search('blax*')
        self.assertNotEqual(query, q)
        self.assertEqual(q.db, db)
        self.assertEqual(q.pattern, 'blax*')
        self.assertTrue(query.count())
        self.assertEqual(query.count(), len(query))
        self.assertEqual(q.count(), 1)
        keys = list(q)
        self.assertEqual(len(keys), 1)
        key = q[0]
        self.assertEqual(str(key), 'blaxxx')
        
    def testQuerySlice(self):
        db = self.newdb()
        db.client.set('blaxxx', 'test')
        db.client.set('blaxyy', 'test2')
        all = db.all()
        self.assertTrue(isinstance(all, list))
        self.assertEqual(len(all), 2)
        self.assertEqual(all, db.query()[:])
        self.assertEqual(all[-1:], db.query()[-1:])
        self.assertEqual(all[-1:1], db.query()[-1:1])
        self.assertEqual(all[-1:2], db.query()[-1:2])
        self.assertEqual(all[-2:1], db.query()[-2:1])
        self.assertEqual(db.query().search('*yy').delete(), 1)
        self.assertEqual(db.query().delete(), 1)
        self.assertEqual(db.all(), [])
        
    def testRedisKeyManager(self):
        db = self.newdb()
        db.client.set('blaxxx', 'test')
        db.client.set('blaxyy', 'test2')
        all = db.all()
        self.assertEqual(len(all), 2)
        self.assertEqual(RedisKey.objects.delete(all), 2)
        self.assertEqual(db.all(), [])
    
    def testdataFormatter(self):
        f = RedisDataFormatter()
        self.assertEqual(f.format_date('bla'), '')
        d = f.format_date(time.time())
        self.assertTrue(d)
        
    def test_tails(self):
        # Make sure we have an 100% coverage
        self.assertFalse(list(self.info._dbs(('dbh',))),[])
        db = RedisDb(self.info.client)
        self.assertEqual(db.id,self.client.db)
        self.assertEqual(str(db),str(db.id))
        