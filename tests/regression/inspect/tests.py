from stdnet import test
from stdnet.utils import populate
from stdnet.exceptions import QuerySetError
from stdnet.orm import model_to_dict

from examples.models import Instrument, Fund, Position


class TestFinanceApplication(test.TestCase):
    
    def setUp(self):
        '''Create Instruments and Funds'''
        orm = self.orm
        orm.register(Instrument)
        orm.register(Fund)
        orm.register(Position)
        
    def testSimple(self):
        d = model_to_dict(Instrument)
        self.assertFalse(d)
        inst = Instrument(name = 'erz12', type = 'future', ccy = 'EUR').save()
        d = model_to_dict(inst)
        self.assertTrue(len(d),3)