'''Test unique fields'''
from random import randint

from stdnet import odm, CommitException
from stdnet.utils import test, populate, zip, range

from examples.models import SimpleModel


SIZE = 200
sports = ['football','rugby','swimming','running','cycling']

codes = set(populate('string',SIZE, min_len = 5, max_len = 20))
SIZE = len(codes)
groups = populate('choice', SIZE, choice_from=sports)
codes = list(codes)

def randomcode(num = 1):
    a = set()
    while len(a) < num:
        a.add(codes[randint(0,len(codes)-1)])
    if num == 1:
        return tuple(a)[0]
    else:
        return a


class TestUniqueFilter(test.TestCase):
    model = SimpleModel
    
    @classmethod
    def after_setup(cls):
        with cls.session().begin() as t:
            for n,g in zip(codes,groups):
                t.add(cls.model(code=n, group=g))
        return t.on_result
    
    def testFilterId(self):
        session = self.session()
        query = session.query(self.model)
        obj = yield query.get(id=2)
        self.assertEqual(obj.id, 2)
        self.assertTrue(obj.code)
        obj2 = yield query.get(code=obj.code)
        self.assertEqual(obj, obj2)
    
    def testBadId(self):
        session = self.session()
        yield self.async.assertRaises(self.model.DoesNotExist,
                                      session.query(self.model).get, id=-1)
    
    def testFilterSimple(self):
        session = self.session()
        query = session.query(self.model)
        for i in range(10):
            code = randomcode()
            qs = yield query.filter(code=code).all()
            self.assertEqual(len(qs), 1)
            self.assertEqual(qs[0].code, code)
            
    def testIdCode(self):
        session = self.session()
        query = session.query(self.model)
        all = yield session.query(self.model).all()
        all2 = yield test.multi_async((query.get(code=m.code) for m in all))
        self.assertEqual(all, all2)
            
    def testExcludeSimple(self):
        session = self.session()
        query = session.query(self.model)
        for i in range(10):
            code = randomcode()
            all = yield query.exclude(code=code).all()
            self.assertEqual(len(all), SIZE-1)
            self.assertFalse(code in set((o.code for o in all)))
            
    def testFilterCodeIn(self):
        session = self.session()
        query = session.query(self.model)
        codes = randomcode(num=3)
        qs = yield query.filter(code__in=codes).all()
        self.assertTrue(qs)
        match = set((m.code for m in qs))
        self.assertEqual(codes, match)
        
    def testExcludeCodeIn(self):
        session = self.session()
        query = session.query(self.model)
        codes = randomcode(num=3)
        qs = yield query.exclude(code__in=codes).all()
        self.assertTrue(qs)
        match = set((m.code for m in qs))
        for code in codes:
            self.assertFalse(code in match)
            
    def testExcludeInclude(self):
        session = self.session()
        query = session.query(self.model)
        codes = randomcode(num = 3)
        qs = yield query.exclude(code__in=codes).filter(code=codes).all()
        self.assertFalse(qs)        
            
    def testTestUnique(self):
        session = self.session()
        query = session.query(self.model)
        yield self.async.assertEqual(query.test_unique('code', 'xxxxxxxxxx'),
                                     'xxxxxxxxxx')
        m = yield query.get(id=1)
        yield self.async.assertEqual(
                            query.test_unique('code', m.code, m), m.code)
        m2 = yield query.get(id = 2)
        yield self.async.assertRaises(ValueError,
                    query.test_unique, 'code', m.code, m2, ValueError)


class TestUniqueCreate(test.TestCase):
    model = SimpleModel
        
    def testAddNew(self):
        session = self.session()
        m = yield session.add(self.model(code='me', group='bla'))
        self.assertEqualId(m, 1)
        self.assertEqual(m.code, 'me')
        # Try to create another one
        m2 = self.model(code='me', group='foo')
        yield self.async.assertRaises(CommitException, session.add, m2)
        query = session.query(self.model)
        yield self.async.assertEqual(query.count(), 1)
        m = yield query.get(code='me')
        self.assertEqualId(m, 1)
        self.assertEqual(m.group, 'bla')
        session.expunge()
        m = yield session.add(self.model(code='me2', group='bla'))
        self.assertEqualId(m, 2)
        query = session.query(self.model)
        yield self.async.assertEqual(query.count(), 2)
    

class TestUniqueChange(test.TestCase):
    model = SimpleModel
    
    def testChangeValue(self):
        session = self.session()
        query = session.query(self.model)
        m = yield session.add(self.model(code='pippo'))
        self.assertTrue(m.id)
        m2 = yield query.get(code='pippo')
        self.assertEqual(m.id, m2.id)
        # Save with different code
        m2.code = 'pippo2'
        yield m2.save()
        m3 = yield query.get(code='pippo2')
        self.assertEqual(m.id, m3.id)
        yield self.async.assertRaises(self.model.DoesNotExist, query.get, code='pippo')