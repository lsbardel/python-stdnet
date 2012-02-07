from stdnet import test
from stdnet.utils import populate, zip, is_string

from examples.models import Instrument, Position
from examples.data import FinanceTest



class TestInstrument(FinanceTest):
    model = Instrument
    
    def setUp(self):
        self.data.create(self)
        
    def testName(self):
        session = self.session()
        qb = session.query(self.model).all()
        qs = session.query(self.model).get_field('name')
        self.assertEqual(qs._get_field,'name')
        result = qs.all()
        self.assertTrue(result)
        for r in result:
            self.assertTrue(is_string(r))
            
    def testId(self):
        session = self.session()
        qb = session.query(self.model).all()
        qs = session.query(self.model).get_field('id')
        self.assertEqual(qs._get_field,'id')
        result = qs.all()
        self.assertTrue(result)
        for r in result:
            self.assertTrue(isinstance(r,int))
            
            
class TestRelated(FinanceTest):
    model = Position
    
    def setUp(self):
        self.data.makePositions(self)
        
    def testInstrument(self):
        session = self.session()
        qs = session.query(self.model).get_field('instrument')
        self.assertEqual(qs._get_field,'instrument')
        result = qs.all()
        self.assertTrue(result)
        for r in result:
            self.assertTrue(type(r),int)
        
    def testFilter(self):
        session = self.session()
        qs = session.query(self.model).get_field('instrument')
        qi = session.query(Instrument).filter(id__in = qs)
        inst = qi.all()
        ids = qs.all()
        self.assertTrue(inst)
        self.assertTrue(len(ids) >= len(inst))
        idset = set(ids)
        self.assertEqual(len(idset),len(inst))
        self.assertEqual(idset,set((i.id for i in inst)))