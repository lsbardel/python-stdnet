import random

import stdnet
from stdnet import test, orm, InvalidTransaction
from examples.models import SimpleModel, Dictionary
from stdnet.utils import populate

LEN = 100
names = populate('string',LEN, min_len = 5, max_len = 20)


class TransactionReceiver(object):
    requests = {}
    def __call__(self, sender, response = None, raw_command = None, **kwargs):
        if raw_command in self.requests:
            self.requests[raw_command]['response'] = response
        else:
            self.requests[raw_command] = kwargs
        

class TestTransactions(test.TestModelBase):
    model = SimpleModel
        
    def testSave(self):
        with SimpleModel.transaction() as t:
            self.assertEqual(t.server,SimpleModel._meta.cursor)
            s = SimpleModel(code = 'test', description = 'just a test')\
                            .save(transaction = t)
            self.assertTrue(s.id)
            self.assertRaises(SimpleModel.DoesNotExist,
                              SimpleModel.objects.get,id=s.id)
            s2 = SimpleModel(code = 'test2', description = 'just a test')\
                                .save(transaction = t)

        all = list(SimpleModel.objects.all())
        self.assertEqual(len(all),2)
        v = SimpleModel.objects.get(code = 'test')
        self.assertEqual(v.description,'just a test')
        
    def testDelete(self):
        s = SimpleModel(code = 'test', description = 'just a test').save()
        self.assertEqual(SimpleModel.objects.get(id = s.id),s)
        s.delete()
        self.assertRaises(SimpleModel.DoesNotExist,
                          SimpleModel.objects.get,id=s.id)
        
    def testIncompatibleModels(self):
        l = stdnet.struct.list('redis://localhost:6379/?db=9')
        s = stdnet.struct.set('redis://localhost:6379/?db=8')
        self.assertRaises(InvalidTransaction,orm.transaction,l,s)
        l = stdnet.struct.list('redis://localhost:6379/?db=9')
        s = stdnet.struct.set('redis://localhost:6378/?db=9')
        self.assertRaises(InvalidTransaction,orm.transaction,l,s)
        self.assertRaises(InvalidTransaction,orm.transaction,SimpleModel,s)
        
    def testCompatibleModels(self):
        s = stdnet.struct.set(SimpleModel._meta.cursor)
        t = orm.transaction(SimpleModel,s, name = 'test-transaction')
        self.assertEqual(t.server,SimpleModel._meta.cursor)
        self.assertEqual(t.name,'test-transaction')
        
    def testSingleTransaction(self):
        db = SimpleModel._meta.cursor
        s = stdnet.struct.set(db)
        l = stdnet.struct.list(db)
        h = stdnet.struct.hash(db)
        r = TransactionReceiver()
        db.redispy.signal_on_send.connect(r)
        db.redispy.signal_on_received.connect(r)
        with orm.transaction(s,l,h,SimpleModel) as t:
            m = SimpleModel(code = 'test', description = 'just a test')
            h.add('test','bla',t)
            l.push_back(5,t)
            l.push_back(8,t)
            s.update((2,3,4,5,6,7),t)
            s.save(t)
            l.save(t)
            h.save(t)
            m.save(t)
        self.assertEqual(s.size(),6)
        self.assertEqual(l.size(),2)
        self.assertEqual(h.size(),1)
        self.assertEqual(SimpleModel.objects.all().count(),1)
        self.assertEqual(len(r.requests),1)
        
        
class TestMultiFieldTransaction(test.TestModelBase):
    model = Dictionary
    
    def make(self):
        with orm.transaction(self.model, name = 'create models') as t:
            self.assertEqual(t.name, 'create models')
            for name in names:
                self.model(name = name).save(t)
            self.assertFalse(t._cachepipes)
    
    def testSaveSimple(self):
        self.make()
        
    def testHashField(self):
        self.make()
        d1,d2 = tuple(self.model.objects.filter(id__in = (1,2)))
        with orm.transaction(self.model) as t:
            d1.data.add('ciao','hello in Italian',t)
            d1.data.add('bla',10000,t)
            d2.data.add('wine','drink to enjoy with or without food',t)
            d2.data.add('foo',98,t)
            self.assertTrue(t._cachepipes)
            # We should have two entries in the cachepipes
            self.assertTrue(len(t._cachepipes),2)
            for c in t._cachepipes:
                self.assertEqual(len(t._cachepipes[c].pipe),2)
                
        self.assertEqual(len(t._cachepipes),2)
        self.assertTrue(t.empty())
        d1,d2 = tuple(self.model.objects.all().sort_by('id'))[:2]
        self.assertEqual(d1.data['ciao'],'hello in Italian')
        self.assertEqual(d2.data['wine'],'drink to enjoy with or without food')
    
    