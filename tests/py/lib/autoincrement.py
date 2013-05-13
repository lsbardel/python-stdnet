from stdnet import odm
from stdnet.apps.searchengine.models import WordItem
from stdnet.utils import test

from examples.models import SimpleModel


class TestCase(test.TestWrite):
    multipledb = 'redis'
    models = (WordItem, SimpleModel)
        
    def testAutoIncrement(self):
        a = odm.autoincrement()
        self.assertEqual(a.incrby, 1)
        self.assertEqual(a.desc, False)
        self.assertEqual(str(a), 'autoincrement(1)')
        a = odm.autoincrement(3)
        self.assertEqual(a.incrby, 3)
        self.assertEqual(a.desc, False)
        self.assertEqual(str(a), 'autoincrement(3)')
        b = -a
        self.assertEqual(str(a), 'autoincrement(3)')
        self.assertEqual(b.desc, True)
        self.assertEqual(str(b), '-autoincrement(3)')
        
    def testSimple(self):
        session = self.session()
        m = yield session.add(SimpleModel(code='pluto'))
        w = yield session.add(WordItem(word='ciao', model_type=SimpleModel,
                                       object_id=m.id))
        yield self.async.assertEqual(session.query(WordItem).count(), 1)
        w = yield session.add(WordItem(word='ciao', model_type=SimpleModel,
                                       object_id=m.id))
        yield self.async.assertEqual(session.query(WordItem).count(), 1)
        self.assertEqual(w.get_state().score, 2)
        #
        w = yield session.add(WordItem(word='ciao', model_type=SimpleModel,
                                       object_id=m.id))
        yield self.async.assertEqual(session.query(WordItem).count(), 1)
        self.assertEqual(w.get_state().score, 3)