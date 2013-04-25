import random

from stdnet import odm, InvalidTransaction
from examples.models import SimpleModel, Dictionary
from stdnet.utils import test, populate

LEN = 100
names = populate('string',LEN, min_len = 5, max_len = 20)


class TransactionReceiver(object):
    
    def __init__(self):
        self.transactions = []
        
    def __call__(self, sender, instances=None, **kwargs):
        self.transactions.append((sender, instances))
        

class TestTransactions(test.TestCase):
    model = SimpleModel
    
    def testCreate(self):
        session = self.session()
        query = session.query(self.model)
        receiver = TransactionReceiver()
        odm.post_commit.connect(receiver, self.model)
        with session.begin() as t:
            self.assertEqual(t.backend, session.backend)
            s = session.add(self.model(code = 'test',
                                       description = 'just a test'))
            self.assertFalse(s.id)
            s2 = session.add(self.model(code = 'test2',
                                   description = 'just a test'))
        yield t.on_result
        all = yield query.filter(code=('test','test2')).all()
        self.assertEqual(len(all), 2)
        self.assertTrue(len(receiver.transactions), 1)
        sender, instances = receiver.transactions[0]
        self.assertEqual(sender, self.model)
        self.assertTrue(instances)
        self.assertEqual(instances, all)
        
    def testDelete(self):
        session = self.session()
        query = session.query(self.model)
        with session.begin() as t:
            s = session.add(self.model(code='bla',
                                       description='just a test'))
        yield t.on_result
        yield self.async.assertEqual(query.get(id = s.id), s)
        yield s.delete()
        yield self.async.assertRaises(self.model.DoesNotExist,
                                      query.get, id=s.id)
        
    def testNoTransaction(self):
        session = self.session()
        s = yield session.add(odm.Set())
        l = yield session.add(odm.List())
        h = yield session.add(odm.HashTable())
        yield self.async.assertEqual(l.size(), 0)
        m = yield session.add(self.model(code='xxxxx', description='just a test'))
        # add an entry to the hashtable
        yield h.add('test', 'bla')
        yield l.push_back(5)
        yield l.push_back(8)
        yield s.update((2,3,4,5,6,7))
        self.assertTrue(m.get_state().persistent)
        yield self.async.assertEqual(s.size(), 6)
        yield self.async.assertEqual(h.size(), 1)
        yield self.async.assertEqual(l.size(), 2)
        m1 = yield session.query(self.model).get(code='xxxxx')
        self.assertEqual(m1, m)
        
    def test_force_update(self):
        session = self.session()
        with session.begin() as t:
             s = session.add(self.model(code='test10',
                                        description='just a test'))
        yield t.on_result
        with session.begin() as t:
            s = t.add(s, force_update=True)
            self.assertEqual(s._force_update, True)
        yield t.on_result
        
        
class TestMultiFieldTransaction(test.TestCase):
    model = Dictionary
    
    def make(self):
        with self.session().begin(name='create models') as t:
            self.assertEqual(t.name, 'create models')
            for name in names:
                t.add(self.model(name=name))
        return t.on_result
        
    def testHashField(self):
        yield self.make()
        session = self.session()
        query = session.query(self.model)
        d1, d2 = yield query.filter(id__in=(1,2)).all()
        with session.begin() as t:
            d1.data.add('ciao','hello in Italian')
            d1.data.add('bla',10000)
            d2.data.add('wine','drink to enjoy with or without food')
            d2.data.add('foo',98)
            self.assertTrue(d1.data.cache.toadd)
            self.assertTrue(d2.data.cache.toadd)
        yield t.on_result
        self.assertFalse(d1.data.cache.toadd)
        self.assertFalse(d2.data.cache.toadd)
        d1, d2 = yield query.filter(id__in=(1,2)).sort_by('id').load_related('data').all()
        self.assertEqual(d1.data['ciao'],'hello in Italian')
        self.assertEqual(d2.data['wine'],'drink to enjoy with or without food')
    
    