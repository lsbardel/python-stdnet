from stdnet.utils import test

from examples.dynamo import Instrument


class PrimaryKey(test.TestCase):
    model = Instrument
    
    def setUp(self):
        self.register()
        
    def testCreate(self):
        m = Instrument(name = 'bla', ccy = 'EUR', type = 'foo').save()
        self.assertEqual(m.name,'bla')