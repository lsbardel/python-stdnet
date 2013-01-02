from stdnet import odm
from stdnet.apps.searchengine.models import WordItem
from stdnet.utils import test

from examples.models import SimpleModel


class TestCase(test.TestCase):
    multipledb = 'redis'
    models = (WordItem, SimpleModel)
    
    def setUp(self):
        self.register()
        
    def testAutoIncrement(self):
        a = odm.autoincrement()
        self.assertEqual(a.incrby,1)
        self.assertEqual(a.desc,False)
        self.assertEqual(str(a),'autoincrement(1)')
        a = odm.autoincrement(3)
        self.assertEqual(a.incrby,3)
        self.assertEqual(a.desc,False)
        self.assertEqual(str(a),'autoincrement(3)')
        b = -a
        self.assertEqual(str(a),'autoincrement(3)')
        self.assertEqual(b.desc,True)
        self.assertEqual(str(b),'-autoincrement(3)')
        
    def testSimple(self):
        m = SimpleModel(code = 'pluto').save()
        w = WordItem(word='ciao', model_type = SimpleModel,
                     object_id = m.id).save()
        self.assertEqual(WordItem.objects.query().count(),1)
        w = WordItem(word='ciao', model_type = SimpleModel,
                     object_id = m.id).save()
        self.assertEqual(WordItem.objects.query().count(),1)
        self.assertEqual(w.state().score,2)
        w = WordItem(word='ciao', model_type = SimpleModel,
                     object_id = m.id).save()
        self.assertEqual(WordItem.objects.query().count(),1)
        self.assertEqual(w.state().score,3)