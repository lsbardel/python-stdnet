from stdnet import test, orm
from stdnet.apps.searchengine.models import Word, WordItem


class TestCase(test.TestCase):
    models = (Word, WordItem)
    
    def setUp(self):
        self.register()
        
    def testAutoIncrement(self):
        a = orm.autoincrement()
        self.assertEqual(a.incrby,1)
        self.assertEqual(a.desc,False)
        self.assertEqual(str(a),'autoincrement(1)')
        a = orm.autoincrement(3)
        self.assertEqual(a.incrby,3)
        self.assertEqual(a.desc,False)
        self.assertEqual(str(a),'autoincrement(3)')
        b = -a
        self.assertEqual(str(a),'autoincrement(3)')
        self.assertEqual(b.desc,True)
        self.assertEqual(str(b),'-autoincrement(3)')
        
    def testSimple(self):
        w = Word(id='ciao').save()
        self.assertEqual(w.id,'ciao')