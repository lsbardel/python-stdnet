from stdnet import FieldError
from stdnet.utils import test

from examples.models import Dictionary
from examples.data import finance_data, Position, Instrument, Fund


class test_load_related(test.TestCase):
    data_cls = finance_data
    models = (Instrument, Fund, Position)
    
    @classmethod
    def setUpClass(cls):
        super(test_load_related, cls).setUpClass()
        cls.data = cls.data_cls(size=cls.size)
        cls.data.makePositions(cls('testMeta'))
        
    @classmethod
    def tearDownClass(cls):
        yield cls.clear_all()

    def testMeta(self):
        session = self.session()
        query = session.query(Position)
        self.assertEqual(query.select_related, None)
        pos1 = query.load_related('instrument')
        self.assertEqual(len(pos1.select_related),1)
        self.assertEqual(pos1.select_related['instrument'], ())
        pos2 = pos1.load_related('instrument','name','ccy')
        self.assertEqual(pos1.select_related['instrument'], ())
        self.assertEqual(pos2.select_related['instrument'], ('name','ccy'))
        pos3 = pos2.load_related('fund','name')
        self.assertEqual(len(pos1.select_related),1)
        self.assertEqual(len(pos2.select_related),1)
        self.assertEqual(len(pos3.select_related),2)
        self.assertEqual(pos1.select_related['instrument'], ())
        self.assertEqual(pos2.select_related['instrument'], ('name','ccy'))
        self.assertEqual(pos3.select_related['instrument'], ('name','ccy'))
        self.assertEqual(pos3.select_related['fund'], ('name',))

    def testSingle(self):
        session = self.session()
        query = session.query(Position)
        pos = query.load_related('instrument')
        fund = Position._meta.dfields['fund']
        inst = Position._meta.dfields['instrument']
        pos = list(pos)
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            cache = fund.get_cache_name()
            val = getattr(p,cache,None)
            self.assertFalse(val)

    def testSingle_withFields(self):
        session = self.session()
        query = session.query(Position)
        pos = query.load_related('instrument', 'name', 'ccy')
        inst = Position._meta.dfields['instrument']
        pos = list(pos)
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            self.assertEqual(set(val._loadedfields),set(('name','ccy')))
            self.assertTrue(val.name)
            self.assertTrue(val.ccy)
            self.assertFalse(hasattr(val,'type'))

    def testDouble(self):
        session = self.session()
        pos = session.query(Position).load_related('instrument')\
                                     .load_related('fund')
        fund = Position._meta.dfields['fund']
        inst = Position._meta.dfields['instrument']
        pos = list(pos)
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            cache = fund.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,fund.relmodel))

    def testError(self):
        session = self.session()
        query = session.query(Position)
        pos = self.assertRaises(FieldError, query.load_related, 'bla')
        pos = self.assertRaises(FieldError, query.load_related, 'size')
        pos = query.load_related('instrument','id')
        self.assertEqual(len(pos.select_related),1)
        self.assertEqual(pos.select_related['instrument'], ())

    def testLoadRelatedLoadOnly(self):
        session = self.session()
        query = session.query(Position)
        inst = Position._meta.dfields['instrument']
        q = query.load_only('dt','size').load_related('instrument')
        self.assertEqual(q.fields,('dt','size'))
        for p in q:
            self.assertEqual(set(p._loadedfields),
                             set(('dt','instrument','size')))
            cache = inst.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))

    def testWithFilter(self):
        session = self.session()
        instruments = session.query(Instrument).filter(ccy='EUR')
        qs = session.query(Position).filter(instrument=instruments)\
                                    .load_related('instrument')
        inst = Position._meta.dfields['instrument']
        for p in qs:
            cache = inst.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val, inst.relmodel))
            self.assertEqual(p.instrument.ccy, 'EUR')


class test_load_related_empty(test.TestCase):
    models = (Instrument, Fund, Position)
    
    def testEmpty(self):
        session = self.session()
        instruments = session.query(Position).load_related('instrument').all()
        self.assertEqual(instruments, [])
        
        
class load_related_structure(test.CleanTestCase):

    def setUp(self):
        session = self.session()
        with session.begin():
            d1 = session.add(Dictionary(name = 'english-italian'))
            d2 = session.add(Dictionary(name = 'italian-english'))
        with session.begin():
            d1.data.update((('ball','palla'),
                            ('boat','nave'),
                            ('cat','gatto')))
            d2.data.update((('palla','ball'),
                            ('nave','boat'),
                            ('gatto','cat')))

    def testGet(self):
        session = self.session()
        query = session.query(Dictionary)
        # Check if data is there first
        d = query.get(name = 'english-italian')
        remote = dict(d.data.items())
        self.assertEqual(len(remote),3)
        #
        d = query.load_related('data').get(name='english-italian')
        data = d.data
        # the cache should be available
        cache = data.cache.cache
        self.assertEqual(len(cache),3)
        self.assertEqual(cache,remote)

