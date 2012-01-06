from random import randint

from stdnet import test, orm
from stdnet.utils import populate, zip, range

from examples.models import SimpleModel


SIZE = 200
sports = ['football','rugby','swimming','running','cycling']

codes = set(populate('string',SIZE, min_len = 5, max_len = 20))
SIZE = len(codes)
groups = populate('choice',SIZE,choice_from=sports)
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
    
    def setUp(self):
        session = self.session()
        with session.begin():
            for n,g in zip(codes,groups):
                session.add(self.model(code = n, group = g))
    
    def testFilterSimple(self):
        session = self.session()
        query = session.query(self.model)
        for i in range(10):
            code = randomcode()
            qs = query.filter(code = code)
            self.assertEqual(qs.count(),1)
            self.assertEqual(qs[0].code,code)
            
    def testExcludeSimple(self):
        session = self.session()
        query = session.query(self.model)
        for i in range(10):
            code = randomcode()
            r = query.exclude(code = code)
            self.assertEqual(r.count(),SIZE-1)
            self.assertFalse(code in set((o.code for o in r)))
            
    def testFilterIn(self):
        session = self.session()
        query = session.query(self.model)
        codes = randomcode(num = 3)
        qs = query.filter(code__in = codes)
        match = set((m.code for m in qs))
        self.assertEqual(codes,match)
        
    def testExcludeIn(self):
        session = self.session()
        query = session.query(self.model)
        codes = randomcode(num = 3)
        qs = query.exclude(code__in = codes)
        match = set((m.code for m in qs))
        for code in codes:
            self.assertFalse(code in match)
            
    def testExcludeInclude(self):
        session = self.session()
        query = session.query(self.model)
        codes = randomcode(num = 3)
        qs = query.exclude(code__in = codes).filter(code__in = codes)
        self.assertEqual(qs.count(),0)        
            
    def testTestUnique(self):
        session = self.session()
        query = session.query(self.model)
        self.assertEqual(query.test_unique('code','xxxxxxxxxx'),'xxxxxxxxxx')
        m = query.get(id = 1)
        self.assertEqual(query.test_unique('code',m.code,m),m.code)
        m2 = query.get(id = 2)
        self.assertRaises(ValueError,
                    query.test_unique,'code',m.code,m2,ValueError)

