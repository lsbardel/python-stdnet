import random

from stdnet import test, orm, InvalidTransaction
from examples.models import SimpleModel, Dictionary
from stdnet.utils import populate

LEN = 100
names = populate('string',LEN, min_len = 5, max_len = 20)


class TransactionReceiver(object):
    
    def __init__(self):
        self.transactions = []
        
    def __call__(self, sender, instances = None, transaction = None, **kwargs):
        self.transactions.append((sender,instances,transaction,
                                  transaction.session))
        

class TestTransactions(test.TestCase):
    model = SimpleModel
    
    def testCreate(self):
        session = self.session()
        query = session.query(self.model)
        receiver = TransactionReceiver()
        orm.post_commit.connect(receiver, orm.Session)
        orm.post_commit.connect(receiver, self.model)
        with session.begin() as t:
            self.assertEqual(t.backend,session.backend)
            s = session.add(self.model(code = 'test',
                                       description = 'just a test'))
            self.assertFalse(s.id)
            s2 = session.add(self.model(code = 'test2',
                                   description = 'just a test'))
            
        all = query.all()
        self.assertEqual(len(all),2)
        self.assertTrue(len(receiver.transactions),2)
        sender,instances,t,s = receiver.transactions[0]
        self.assertEqual(sender, orm.Session)
        self.assertEqual(s, session)
        sender,instances,t,s = receiver.transactions[1]
        self.assertTrue(instances)
        self.assertEqual(instances,all)
        self.assertEqual(sender,self.model)
        
    def testDelete(self):
        session = self.session()
        query = session.query(self.model)
        with session.begin():
            s = session.add(self.model(code = 'test',
                                       description = 'just a test'))
        self.assertEqual(query.count(),1)
        self.assertEqual(query.get(id = s.id),s)
        s.delete()
        self.assertRaises(self.model.DoesNotExist,
                          query.get, id=s.id)
        
    def testNoTransaction(self):
        session = self.session()
        s = session.add(orm.Set())
        l = session.add(orm.List())
        h = session.add(orm.HashTable())
        self.assertEqual(l.size(),0)
        r = TransactionReceiver()
        orm.post_commit.connect(r, orm.Session)
        m = session.add(self.model(code = 'test', description = 'just a test'))
        # add an entry to the hashtable
        h.add('test','bla')
        self.assertEqual(len(r.transactions),2)
        l.push_back(5)
        l.push_back(8)
        s.update((2,3,4,5,6,7))
        self.assertEqual(len(r.transactions),5)
        self.assertTrue(m.state().persistent)
        self.assertEqual(s.size(),6)
        self.assertEqual(h.size(),1)
        self.assertEqual(l.size(),2)
        self.assertEqual(len(session.query(self.model).all()),1)
        
        
class TestMultiFieldTransaction(test.TestCase):
    model = Dictionary
    
    def make(self):
        with self.session().begin(name = 'create models') as t:
            self.assertEqual(t.name, 'create models')
            for name in names:
                t.add(self.model(name = name))
    
    def testSaveSimple(self):
        self.make()
        
    def testHashField(self):
        self.make()
        session = self.session()
        query = session.query(self.model)
        d1,d2 = tuple(query.filter(id__in = (1,2)))
        with session.begin():
            d1.data.add('ciao','hello in Italian')
            d1.data.add('bla',10000)
            d2.data.add('wine','drink to enjoy with or without food')
            d2.data.add('foo',98)
            self.assertTrue(d1.data.cache.toadd)
            self.assertTrue(d2.data.cache.toadd)
                
        self.assertFalse(d1.data.cache.toadd)
        self.assertFalse(d2.data.cache.toadd)
        d1,d2 = tuple(query.sort_by('id'))[:2]
        self.assertEqual(d1.data['ciao'],'hello in Italian')
        self.assertEqual(d2.data['wine'],'drink to enjoy with or without food')
    
    