from stdnet import test

from examples.models import Dictionary
from examples.data import FinanceTest, Position, Instrument, Fund


class load_related(FinanceTest):
    
    def testMeta(self):
        self.data.makePositions(self)
        session = self.session()
        query = session.query(Position)
        self.assertEqual(query.select_related, None)
        pos1 = query.load_related('instrument')
        self.assertEqual(len(pos1.select_related),1)
        self.assertEqual(pos1.select_related['instrument'],set())
        pos2 = pos1.load_related('instrument','name','ccy')
        self.assertEqual(pos1.select_related['instrument'],set())
        self.assertEqual(pos2.select_related['instrument'],set(('name','ccy')))
        pos3 = pos2.load_related('fund','name')
        self.assertEqual(len(pos1.select_related),1)
        self.assertEqual(len(pos2.select_related),1)
        self.assertEqual(len(pos3.select_related),2)
        self.assertEqual(pos1.select_related['instrument'],set())
        self.assertEqual(pos2.select_related['instrument'],set(('name','ccy')))
        self.assertEqual(pos3.select_related['instrument'],set(('name','ccy')))
        self.assertEqual(pos3.select_related['fund'],set(('name',)))
        
    def testSingle(self):
        self.data.makePositions(self)
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
        self.data.makePositions(self)
        session = self.session()
        query = session.query(Position)
        pos = query.load_related('instrument','name','ccy')
        inst = Position._meta.dfields['instrument']
        pos = list(pos)
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            self.assertEqual(set(val._loadedfields),set(('name','ccy')))
            self.assertTrue(val.name)
            self.assertTrue(val.ccy)
            self.assertFalse(hasattr(val,'type'))
            
    def testDouble(self):
        self.data.makePositions(self)
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
    

class load_related_structure(test.TestCase):
    
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
        d = query.load_related('data').get(name = 'english-italian')
        data = d.data
        # the cache should be available
        cache = data.cache.cache
        self.assertEqual(len(cache),3)
        self.assertEqual(cache,remote)
        
    