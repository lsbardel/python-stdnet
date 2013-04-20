from stdnet.utils import test
from stdnet.utils.populate import populate
from stdnet.utils.py2py3 import zip

from examples.models import NumericData, CrossData, Feed1
from examples.data import data_generator


class NumberGenerator(data_generator):
    
    def generate(self, **kwargs):
        self.d1 = populate('integer', self.size, start=-5, end=5)
        self.d2 = populate('float', self.size, start=-10, end=10)
        self.d3 = populate('float', self.size, start=-10, end=10)
        self.d4 = populate('float', self.size, start=-10, end=10)
        self.d5 = populate('integer', self.size, start=-5, end=5)
        self.d6 = populate('integer', self.size, start=-5, end=5)
        
        
class NumericTest(test.TestCase):
    multipledb = ['redis', 'mongo']
    data_cls = NumberGenerator
    models = (NumericData,)

    @classmethod
    def after_setup(cls):
        cls.data = cls.data_cls(size=cls.size)
        cls.register()
        d = cls.data
        with cls.session().begin() as t:
            for a, b, c, d, e, f in zip(d.d1, d.d2, d.d3, d.d4, d.d5, d.d6):
                t.add(cls.model(pv=a, vega=b, delta=c, gamma=d,
                                data={'test': {'': e,
                                               'inner': f}}))
        yield t.on_result
    

class TestNumericRange(NumericTest):
                
    def testGT(self):
        session = self.session()
        qs = session.query(NumericData).filter(pv__gt=1)
        qs = yield qs.all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv > 1)
        qs = yield session.query(NumericData).filter(pv__gt=-2).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv > -2)
    
    def testGE(self):
        session = self.session()
        qs = yield session.query(NumericData).filter(pv__ge=-2).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv >= -2)
        qs = yield session.query(NumericData).filter(pv__ge=0).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv >= 0)
            
    def testLT(self):
        session = self.session()
        qs = yield session.query(NumericData).filter(pv__lt=2).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv < 2)
        qs = yield session.query(NumericData).filter(pv__lt=-1).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv < -1)
            
    def testLE(self):
        session = self.session()
        qs = yield session.query(NumericData).filter(pv__le=1).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv <= 1)
        qs = yield session.query(NumericData).filter(pv__le=-1).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv <= -1)
            
    def testMix(self):
        session = self.session()
        qs = yield session.query(NumericData).filter(pv__gt=1, pv__lt=0).all()
        self.assertFalse(qs)
        qs = yield session.query(NumericData).filter(pv__ge=-2, pv__lt=3).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv < 3)
            self.assertTrue(v.pv >= -2)
        
    def testMoreThanOne(self):
        session = self.session()
        qs = yield session.query(NumericData).filter(pv__ge=-2, pv__lt=3)\
                                             .filter(vega__gt=0).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv < 3)
            self.assertTrue(v.pv >= -2)
            self.assertTrue(v.vega > 0)
    
    def testWithString(self):
        session = self.session()
        qs = yield session.query(NumericData).filter(pv__ge='-2').all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.pv >= -2)
        
    def testJson(self):
        session = self.session()
        qs = yield session.query(NumericData).filter(data__test__gt=1).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.data__test > 1)
        qs = yield session.query(NumericData).filter(data__test__gt='-2').all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.data__test > -2)
        qs = yield session.query(NumericData).filter(data__test__inner__gt='1').all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.data__test__inner > 1)
        qs = yield session.query(NumericData).filter(data__test__inner__gt=-2).all()
        self.assertTrue(qs)
        for v in qs:
            self.assertTrue(v.data__test__inner > -2)
            

class TestNumericRangeForeignKey(test.TestCase):
    multipledb = ['redis', 'mongo']
    data_cls = NumberGenerator
    models = (CrossData, Feed1)

    @classmethod
    def after_setup(cls):
        cls.data = cls.data_cls(size=cls.size)
        cls.register()
        yield cls.clear_all()
        session = cls.session()
        da = cls.data
        with session.begin() as t:
            for a, b, c, d, e, f in zip(da.d1, da.d2, da.d3, da.d4, da.d5, da.d6):
                t.add(CrossData(name='live',
                                data={'a': a, 'b': b, 'c': c,
                                      'd': d, 'e': e, 'f': f}))
        yield t.on_result
        cross = yield CrossData.objects.query().all()
        found = False
        with session.begin() as t:
            for n, c in enumerate(cross):
                if c.data__a > -1:
                    found=True
                feed = 'feed%s' % (n+1)
                t.add(Feed1(name=feed, live=c))
        yield t.on_result
        assert found, 'not found'
    
    def test_feeds(self):
        qs = yield Feed1.objects.query().load_related('live').all()
        self.assertTrue(qs)
        for feed in qs:
            self.assertTrue(feed.live)
            self.assertTrue(isinstance(feed.live.data, dict))
        qs = yield CrossData.objects.filter(data__a__gt=-1).all()
        self.assertTrue(qs)
        for c in qs:
            self.assertTrue(c.data__a >= -1)
            
    def test_gt_direct(self):
        qs1 = CrossData.objects.filter(data__a__gt=-1)
        qs = yield Feed1.objects.filter(live=qs1).load_related('live').all()
        self.assertTrue(qs)
        for feed in qs:
            self.assertTrue(feed.live.data__a >= -1)
            
    def test_gt(self):
        qs = yield Feed1.objects.filter(live__data__a__gt=-1).load_related('live').all()
        self.assertTrue(qs)
        for feed in qs:
            self.assertTrue(feed.live.data__a >= -1)