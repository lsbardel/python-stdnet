from stdnet import odm, FieldError
from stdnet.utils import test

from examples.models import Dictionary, Profile
from examples.data import FinanceTest, Position, Instrument, Fund


class Role(odm.StdModel):
    profile = odm.ForeignKey(Profile)


class test_load_related(FinanceTest):
    
    @classmethod
    def after_setup(cls):
        yield cls.data.makePositions(cls)
        
    def testMeta(self):
        session = self.session()
        query = session.query(Position)
        self.assertEqual(query.select_related, None)
        pos1 = query.load_related('instrument')
        self.assertEqual(len(pos1.select_related), 1)
        self.assertEqual(pos1.select_related['instrument'], ())
        pos2 = pos1.load_related('instrument', 'name', 'ccy')
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
        pos = yield pos.all()
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            cache = fund.get_cache_name()
            val = getattr(p,cache,None)
            self.assertFalse(val)

    def test_single_with_fields(self):
        session = self.session()
        query = session.query(Position)
        pos = query.load_related('instrument', 'name', 'ccy')
        inst = Position._meta.dfields['instrument']
        pos = yield pos.all()
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val, inst.relmodel))
            self.assertEqual(set(val._loadedfields),set(('name','ccy')))
            self.assertTrue(val.name)
            self.assertTrue(val.ccy)
            self.assertFalse(hasattr(val,'type'))
            
    def test_with_id_only(self):
        '''Test load realated when loading only the id'''
        session = self.session()
        query = session.query(Position)
        pos = query.load_related('instrument', 'id')
        inst = Position._meta.dfields['instrument']
        pos = yield pos.all()
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val, inst.relmodel))
            self.assertFalse(hasattr(val,'name'))
            self.assertFalse(hasattr(val,'ccy'))
            self.assertFalse(hasattr(val,'type'))
            self.assertEqual(set(val._loadedfields), set())

    def testDouble(self):
        session = self.session()
        pos = session.query(Position).load_related('instrument')\
                                     .load_related('fund')
        fund = Position._meta.dfields['fund']
        inst = Position._meta.dfields['instrument']
        pos = yield pos.all()
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            cache = fund.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val, fund.relmodel))

    def testError(self):
        session = self.session()
        query = session.query(Position)
        pos = self.assertRaises(FieldError, query.load_related, 'bla')
        pos = self.assertRaises(FieldError, query.load_related, 'size')
        pos = query.load_related('instrument', 'id')
        self.assertEqual(len(pos.select_related), 1)
        self.assertEqual(pos.select_related['instrument'], ('id',))

    def testLoadRelatedLoadOnly(self):
        session = self.session()
        query = session.query(Position)
        inst = Position._meta.dfields['instrument']
        qs = query.load_only('dt','size').load_related('instrument')
        self.assertEqual(qs.fields, ('dt','size'))
        qs = yield qs.all()
        self.assertTrue(qs)
        for p in qs:
            self.assertEqual(set(p._loadedfields),
                             set(('dt','instrument','size')))
            cache = inst.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val, inst.relmodel))

    def test_with_filter(self):
        session = self.session()
        instruments = session.query(Instrument).filter(ccy='EUR')
        qs = session.query(Position).filter(instrument=instruments)\
                                    .load_related('instrument')
        inst = Position._meta.dfields['instrument']
        qs = yield qs.all()
        self.assertTrue(qs)
        for p in qs:
            cache = inst.get_cache_name()
            val = getattr(p, cache, None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val, inst.relmodel))
            self.assertEqual(p.instrument.ccy, 'EUR')


class test_load_related_empty(test.TestCase):
    models = (Role, Profile, Position, Instrument, Fund)
    
    @classmethod
    def after_setup(cls):
        with cls.session().begin() as t:
            p1 = t.add(Profile())
            p2 = t.add(Profile())
            p3 = t.add(Profile())
        yield t.on_result
        with cls.session().begin() as t:
            t.add(Role(profile=p1))
            t.add(Role(profile=p1))
            t.add(Role(profile=p3))
        yield t.on_result
        
    def testEmpty(self):
        models = self.mapper
        insts = yield models.position.query().load_related('instrument').all()
        self.assertEqual(insts, [])
        
    def test_related_no_fields(self):
        qs = self.query().load_related('profile')
        query = yield qs.all()
        profiles = set((role.profile for role in query))
        self.assertEqual(len(profiles), 2)
    
 
class load_related_structure(test.TestCase):
    model = Dictionary
    
    @classmethod
    def after_setup(cls):
        with cls.session().begin() as t:
            d1 = t.add(Dictionary(name='english-italian'))
            d2 = t.add(Dictionary(name='italian-english'))
        yield t.on_result
        with cls.session().begin() as t:
            d1.data.update((('ball','palla'),
                            ('boat','nave'),
                            ('cat','gatto')))
            d2.data.update((('palla','ball'),
                            ('nave','boat'),
                            ('gatto','cat')))
        yield t.on_result

    def test_hash(self):
        session = self.session()
        query = session.query(Dictionary)
        # Check if data is there first
        d = yield query.get(name = 'english-italian')
        data = yield d.data.items()
        remote = dict(data)
        self.assertEqual(len(remote), 3)
        #
        d = yield query.load_related('data').get(name='english-italian')
        data = d.data
        # the cache should be available
        cache = data.cache.cache
        self.assertEqual(len(cache), 3)
        self.assertEqual(cache, remote)

