from stdnet import test, getdb
from stdnet.lib.redisinfo import redis_info


class TestInfo(test.TestCase):
    
    def setUp(self):
        rpy = getdb().redispy
        rpy.set('test','bla')
        self.db = rpy.db
        self.info = redis_info(rpy)
        
    def testSimple(self):
        info = self.info
        self.assertTrue(info.rpy)
        self.assertEqual(info.rpy.db,self.db)
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
        db = dbs[0]
        self.assertEqual(db.rpy,info.rpy)
    