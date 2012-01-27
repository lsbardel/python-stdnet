from stdnet.lib.redis import redis_info

from .base import TestCase


class TestInfo(TestCase):
    
    def setUp(self):
        super(TestInfo,self).setUp()
        self.client.set('test','bla')
        self.db = self.client.db
        self.info = redis_info(self.client)
        
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
    