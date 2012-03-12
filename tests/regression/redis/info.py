from stdnet.lib.redis import redis_info, RedisDb, RedisKey

from .base import TestCase


class TestInfo(TestCase):
    
    def setUp(self):
        super(TestInfo,self).setUp()
        self.client.set('test','bla')
        self.db = self.client.db
        self.info = redis_info(self.client)
        
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
            keys = RedisKey.objects.all(db)
            self.assertTrue(keys)
    
    def test_tails(self):
        # Make sure we have an 100% coverage
        self.assertFalse(list(self.info._dbs(('dbh',))),[])
        db = RedisDb(self.info.client)
        self.assertEqual(db.id,self.client.db)
        self.assertEqual(str(db),str(db.id))
        